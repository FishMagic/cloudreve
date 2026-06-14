package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs/dbfs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/workflows"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type JsonRpcRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	ID      interface{}   `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type JsonRpcResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   *JsonRpcErr `json:"error,omitempty"`
}

type JsonRpcErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Aria2TaskStatus struct {
	Gid             string          `json:"gid"`
	Status          string          `json:"status"` // active, waiting, paused, error, complete, removed
	TotalLength     string          `json:"totalLength"`
	CompletedLength string          `json:"completedLength"`
	UploadLength    string          `json:"uploadLength"`
	DownloadSpeed   string          `json:"downloadSpeed"`
	UploadSpeed     string          `json:"uploadSpeed"`
	Files           []Aria2TaskFile `json:"files"`
}

type Aria2TaskFile struct {
	Index           string         `json:"index"`
	Path            string         `json:"path"`
	Length          string         `json:"length"`
	CompletedLength string         `json:"completedLength"`
	Selected        string         `json:"selected"`
	Uris            []Aria2TaskUri `json:"uris"`
}

type Aria2TaskUri struct {
	Uri    string `json:"uri"`
	Status string `json:"status"` // used, waiting
}

func CleanVirtualPath(dir string) string {
	dir = strings.ReplaceAll(dir, "\\", "/")
	if len(dir) >= 2 && dir[1] == ':' {
		dir = dir[2:]
	}
	parts := strings.Split(dir, "/")
	var cleaned []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || p == "." || p == ".." {
			continue
		}
		cleaned = append(cleaned, p)
	}
	if len(cleaned) == 0 {
		return "cloudreve://my/"
	}
	return "cloudreve://my/" + strings.Join(cleaned, "/") + "/"
}

func getAria2Token(c *gin.Context, req *JsonRpcRequest) string {
	token := c.Query("token")
	if token != "" {
		return token
	}

	if req != nil && len(req.Params) > 0 {
		if tokenStr, ok := req.Params[0].(string); ok && strings.HasPrefix(tokenStr, "token:") {
			return strings.TrimPrefix(tokenStr, "token:")
		}
	}

	return ""
}

func getMethodParams(req *JsonRpcRequest) []interface{} {
	if len(req.Params) > 0 {
		if tokenStr, ok := req.Params[0].(string); ok && strings.HasPrefix(tokenStr, "token:") {
			return req.Params[1:]
		}
	}
	return req.Params
}

func mapTaskToAria2(t *ent.Task, hasher hashid.Encoder) Aria2TaskStatus {
	gid := hashid.EncodeTaskID(hasher, t.ID)
	statusStr := "waiting"
	switch t.Status {
	case task.StatusQueued:
		statusStr = "waiting"
	case task.StatusProcessing:
		statusStr = "active"
	case task.StatusSuspending:
		statusStr = "active"
	case task.StatusCompleted:
		statusStr = "complete"
	case task.StatusError:
		statusStr = "error"
	case task.StatusCanceled:
		statusStr = "removed"
	}

	totalLength := "0"
	completedLength := "0"
	downloadSpeed := "0"
	fileName := "Unknown"

	var state workflows.RemoteDownloadTaskState
	if err := json.Unmarshal([]byte(t.PrivateState), &state); err == nil {
		if state.Status != nil {
			totalLength = strconv.FormatInt(state.Status.Total, 10)
			completedLength = strconv.FormatInt(state.Status.Downloaded, 10)
			downloadSpeed = strconv.FormatInt(state.Status.DownloadSpeed, 10)
			fileName = state.Status.Name
		} else if state.SrcUri != "" {
			fileName = state.SrcUri
		}
	}

	if t.Status == task.StatusCompleted {
		completedLength = totalLength
	}

	files := []Aria2TaskFile{
		{
			Index:           "1",
			Path:            fileName,
			Length:          totalLength,
			CompletedLength: completedLength,
			Selected:        "true",
			Uris: []Aria2TaskUri{
				{
					Uri:    state.SrcUri,
					Status: "used",
				},
			},
		},
	}

	return Aria2TaskStatus{
		Gid:             gid,
		Status:          statusStr,
		TotalLength:     totalLength,
		CompletedLength: completedLength,
		UploadLength:    "0",
		DownloadSpeed:   downloadSpeed,
		UploadSpeed:     "0",
		Files:           files,
	}
}

func handleAria2Rpc(c *gin.Context, req *JsonRpcRequest) *JsonRpcResponse {
	dep := dependency.FromContext(c)
	token := getAria2Token(c, req)
	if token == "" {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      req.ID,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Unauthorized: missing token",
			},
		}
	}

	ctx := context.WithValue(c.Request.Context(), inventory.LoadUserGroup{}, true)
	user, err := dep.UserClient().GetActiveByAria2Key(ctx, token, dep.ConfigProvider().Database().Type)
	if err != nil {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      req.ID,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Unauthorized: invalid token",
			},
		}
	}

	if !user.Edges.Group.Permissions.Enabled(int(types.GroupPermissionRemoteDownload)) {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      req.ID,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Forbidden: group not allowed to download files",
			},
		}
	}

	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), inventory.UserCtx{}, user))
	params := getMethodParams(req)

	switch req.Method {
	case "aria2.addUri":
		return handleAddUri(c, req.ID, params, user, dep)
	case "aria2.tellActive":
		return handleTellActive(c, req.ID, user, dep)
	case "aria2.tellWaiting":
		return handleTellWaiting(c, req.ID, user, dep)
	case "aria2.tellStopped":
		return handleTellStopped(c, req.ID, user, dep)
	case "aria2.tellStatus":
		return handleTellStatus(c, req.ID, params, user, dep)
	case "aria2.getGlobalStat":
		return handleGetGlobalStat(c, req.ID, user, dep)
	case "aria2.remove", "aria2.forceRemove":
		return handleRemove(c, req.ID, params, user, dep)
	case "aria2.getVersion":
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"version":         "1.36.0",
				"enabledFeatures": []string{"https", "gzip"},
			},
		}
	case "aria2.getSessionInfo":
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"sessionId": "cloudreve-passthrough-session",
			},
		}
	case "system.listMethods":
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      req.ID,
			Result: []string{
				"aria2.addUri",
				"aria2.tellActive",
				"aria2.tellWaiting",
				"aria2.tellStopped",
				"aria2.tellStatus",
				"aria2.getGlobalStat",
				"aria2.remove",
				"aria2.forceRemove",
				"aria2.getVersion",
				"aria2.getSessionInfo",
				"system.listMethods",
			},
		}
	default:
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      req.ID,
			Error: &JsonRpcErr{
				Code:    -32601,
				Message: fmt.Sprintf("Method not found: %s", req.Method),
			},
		}
	}
}

func handleAddUri(c *gin.Context, id interface{}, params []interface{}, user *ent.User, dep dependency.Dep) *JsonRpcResponse {
	if len(params) == 0 {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Invalid params: missing uris",
			},
		}
	}

	var uris []string
	urisInterface, ok := params[0].([]interface{})
	if ok {
		for _, u := range urisInterface {
			if s, ok := u.(string); ok {
				uris = append(uris, s)
			}
		}
	} else {
		if s, ok := params[0].(string); ok {
			uris = []string{s}
		}
	}

	if len(uris) == 0 {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Invalid params: empty uris",
			},
		}
	}

	options := make(map[string]interface{})
	if len(params) > 1 {
		if optMap, ok := params[1].(map[string]interface{}); ok {
			options = optMap
		}
	}

	dstPath := "cloudreve://my/"
	if dirVal, ok := options["dir"].(string); ok && dirVal != "" {
		dstPath = CleanVirtualPath(dirVal)
	}

	ctx := c.Request.Context()
	dstUri, err := fs.NewUriFromString(dstPath)
	if err != nil {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: fmt.Sprintf("Invalid destination path: %s", err),
			},
		}
	}

	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	_, err = m.Get(ctx, dstUri, dbfs.WithRequiredCapabilities(dbfs.NavigatorCapabilityCreateFile))
	if err != nil {
		_, err = m.Create(ctx, dstUri, types.FileTypeFolder)
		if err != nil {
			return &JsonRpcResponse{
				Jsonrpc: "2.0",
				ID:      id,
				Error: &JsonRpcErr{
					Code:    -32603,
					Message: fmt.Sprintf("Failed to create destination folder recursively: %s", err),
				},
			}
		}
	}

	delete(options, "dir")

	uri := uris[0]
	t, err := workflows.NewRemoteDownloadTask(ctx, uri, "", dstPath, options)
	if err != nil {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32603,
				Message: fmt.Sprintf("Failed to create remote download task: %s", err),
			},
		}
	}

	if err := dep.RemoteDownloadQueue(ctx).QueueTask(ctx, t); err != nil {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32603,
				Message: fmt.Sprintf("Failed to queue remote download task: %s", err),
			},
		}
	}

	gid := hashid.EncodeTaskID(dep.HashIDEncoder(), t.ID())
	return &JsonRpcResponse{
		Jsonrpc: "2.0",
		ID:      id,
		Result:  gid,
	}
}

func handleTellActive(c *gin.Context, id interface{}, user *ent.User, dep dependency.Dep) *JsonRpcResponse {
	taskClient := dep.TaskClient()
	args := &inventory.ListTaskArgs{
		PaginationArgs: &inventory.PaginationArgs{
			PageSize: 1000,
		},
		Types:  []string{queue.RemoteDownloadTaskType},
		UserID: user.ID,
		Status: []task.Status{task.StatusProcessing, task.StatusSuspending},
	}

	res, err := taskClient.List(c.Request.Context(), args)
	if err != nil {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32603,
				Message: fmt.Sprintf("Failed to list active tasks: %s", err),
			},
		}
	}

	hasher := dep.HashIDEncoder()
	var result []Aria2TaskStatus
	for _, t := range res.Tasks {
		result = append(result, mapTaskToAria2(t, hasher))
	}

	return &JsonRpcResponse{
		Jsonrpc: "2.0",
		ID:      id,
		Result:  result,
	}
}

func handleTellWaiting(c *gin.Context, id interface{}, user *ent.User, dep dependency.Dep) *JsonRpcResponse {
	taskClient := dep.TaskClient()
	args := &inventory.ListTaskArgs{
		PaginationArgs: &inventory.PaginationArgs{
			PageSize: 1000,
		},
		Types:  []string{queue.RemoteDownloadTaskType},
		UserID: user.ID,
		Status: []task.Status{task.StatusQueued},
	}

	res, err := taskClient.List(c.Request.Context(), args)
	if err != nil {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32603,
				Message: fmt.Sprintf("Failed to list waiting tasks: %s", err),
			},
		}
	}

	hasher := dep.HashIDEncoder()
	var result []Aria2TaskStatus
	for _, t := range res.Tasks {
		result = append(result, mapTaskToAria2(t, hasher))
	}

	return &JsonRpcResponse{
		Jsonrpc: "2.0",
		ID:      id,
		Result:  result,
	}
}

func handleTellStopped(c *gin.Context, id interface{}, user *ent.User, dep dependency.Dep) *JsonRpcResponse {
	taskClient := dep.TaskClient()
	args := &inventory.ListTaskArgs{
		PaginationArgs: &inventory.PaginationArgs{
			PageSize: 1000,
		},
		Types:  []string{queue.RemoteDownloadTaskType},
		UserID: user.ID,
		Status: []task.Status{task.StatusCompleted, task.StatusError, task.StatusCanceled},
	}

	res, err := taskClient.List(c.Request.Context(), args)
	if err != nil {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32603,
				Message: fmt.Sprintf("Failed to list stopped tasks: %s", err),
			},
		}
	}

	hasher := dep.HashIDEncoder()
	var result []Aria2TaskStatus
	for _, t := range res.Tasks {
		result = append(result, mapTaskToAria2(t, hasher))
	}

	return &JsonRpcResponse{
		Jsonrpc: "2.0",
		ID:      id,
		Result:  result,
	}
}

func handleTellStatus(c *gin.Context, id interface{}, params []interface{}, user *ent.User, dep dependency.Dep) *JsonRpcResponse {
	if len(params) == 0 {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Invalid params: missing gid",
			},
		}
	}

	gid, ok := params[0].(string)
	if !ok || gid == "" {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Invalid params: invalid gid type",
			},
		}
	}

	hasher := dep.HashIDEncoder()
	taskID, err := hasher.Decode(gid, hashid.TaskID)
	if err != nil {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Invalid params: invalid gid",
			},
		}
	}

	taskClient := dep.TaskClient()
	t, err := taskClient.GetTaskByID(c.Request.Context(), taskID)
	if err != nil || (t.Edges.User != nil && t.Edges.User.ID != user.ID) || t.UserTasks != user.ID {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Task not found",
			},
		}
	}

	return &JsonRpcResponse{
		Jsonrpc: "2.0",
		ID:      id,
		Result:  mapTaskToAria2(t, hasher),
	}
}

func handleRemove(c *gin.Context, id interface{}, params []interface{}, user *ent.User, dep dependency.Dep) *JsonRpcResponse {
	if len(params) == 0 {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Invalid params: missing gid",
			},
		}
	}

	gid, ok := params[0].(string)
	if !ok || gid == "" {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Invalid params: invalid gid type",
			},
		}
	}

	hasher := dep.HashIDEncoder()
	taskID, err := hasher.Decode(gid, hashid.TaskID)
	if err != nil {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32602,
				Message: "Invalid params: invalid gid",
			},
		}
	}

	ctx := c.Request.Context()
	r := dep.TaskRegistry()
	t, found := r.Get(taskID)
	if found && t.Owner().ID == user.ID {
		if downloadTask, ok := t.(*workflows.RemoteDownloadTask); ok {
			if err := downloadTask.CancelDownload(ctx); err != nil {
				return &JsonRpcResponse{
					Jsonrpc: "2.0",
					ID:      id,
					Error: &JsonRpcErr{
						Code:    -32603,
						Message: fmt.Sprintf("Failed to cancel active download: %s", err),
					},
				}
			}
		}
	}

	dbTask, err := dep.TaskClient().GetTaskByID(ctx, taskID)
	if err == nil && ((dbTask.Edges.User != nil && dbTask.Edges.User.ID == user.ID) || dbTask.UserTasks == user.ID) {
		if dbTask.Status == task.StatusProcessing || dbTask.Status == task.StatusQueued || dbTask.Status == task.StatusSuspending {
			_, _ = dep.TaskClient().Update(ctx, dbTask, &inventory.TaskArgs{
				Status: task.StatusCanceled,
			})
		}
	}

	return &JsonRpcResponse{
		Jsonrpc: "2.0",
		ID:      id,
		Result:  gid,
	}
}

func handleGetGlobalStat(c *gin.Context, id interface{}, user *ent.User, dep dependency.Dep) *JsonRpcResponse {
	taskClient := dep.TaskClient()
	args := &inventory.ListTaskArgs{
		PaginationArgs: &inventory.PaginationArgs{
			PageSize: 1000,
		},
		Types:  []string{queue.RemoteDownloadTaskType},
		UserID: user.ID,
	}

	res, err := taskClient.List(c.Request.Context(), args)
	if err != nil {
		return &JsonRpcResponse{
			Jsonrpc: "2.0",
			ID:      id,
			Error: &JsonRpcErr{
				Code:    -32603,
				Message: fmt.Sprintf("Failed to list tasks for global stat: %s", err),
			},
		}
	}

	var numActive int
	var numWaiting int
	var numStopped int
	var downloadSpeed int64

	for _, t := range res.Tasks {
		switch t.Status {
		case task.StatusQueued:
			numWaiting++
		case task.StatusProcessing, task.StatusSuspending:
			numActive++
			var state workflows.RemoteDownloadTaskState
			if err := json.Unmarshal([]byte(t.PrivateState), &state); err == nil && state.Status != nil {
				downloadSpeed += state.Status.DownloadSpeed
			}
		case task.StatusCompleted, task.StatusError, task.StatusCanceled:
			numStopped++
		}
	}

	return &JsonRpcResponse{
		Jsonrpc: "2.0",
		ID:      id,
		Result: map[string]string{
			"downloadSpeed": strconv.FormatInt(downloadSpeed, 10),
			"uploadSpeed":   "0",
			"numActive":     strconv.Itoa(numActive),
			"numWaiting":    strconv.Itoa(numWaiting),
			"numStopped":    strconv.Itoa(numStopped),
		},
	}
}

func Aria2RpcHandler(c *gin.Context) {
	if websocket.IsWebSocketUpgrade(c.Request) {
		handleWebSocket(c)
		return
	}

	var req JsonRpcRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusOK, JsonRpcResponse{
			Jsonrpc: "2.0",
			Error: &JsonRpcErr{
				Code:    -32700,
				Message: "Parse error",
			},
		})
		return
	}

	res := handleAria2Rpc(c, &req)
	c.JSON(http.StatusOK, res)
}

func handleWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			break
		}

		if messageType != websocket.TextMessage {
			continue
		}

		var req JsonRpcRequest
		if err := json.Unmarshal(message, &req); err != nil {
			resBytes, _ := json.Marshal(JsonRpcResponse{
				Jsonrpc: "2.0",
				Error: &JsonRpcErr{
					Code:    -32700,
					Message: "Parse error",
				},
			})
			_ = conn.WriteMessage(websocket.TextMessage, resBytes)
			continue
		}

		res := handleAria2Rpc(c, &req)
		resBytes, _ := json.Marshal(res)
		_ = conn.WriteMessage(websocket.TextMessage, resBytes)
	}
}
