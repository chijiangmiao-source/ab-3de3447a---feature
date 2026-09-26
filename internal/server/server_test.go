package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vacuum-interlock/internal/causality"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(New(causality.NewEngine()))
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, url string, body any) (int, map[string]any) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestHealthAndIndex(t *testing.T) {
	srv := newTestServer(t)
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health: %d", resp.StatusCode)
	}
	resp, err = http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body[:n]), "联锁") {
		t.Fatalf("index page: %d", resp.StatusCode)
	}
}

func TestAPIFlow(t *testing.T) {
	srv := newTestServer(t)

	// 建立轮次
	code, out := post(t, srv.URL+"/api/rounds", map[string]any{
		"id":       "r1",
		"consoles": []string{"alpha", "beta"},
		"valves":   map[string]string{"GV1": "closed", "GV2": "closed"},
	})
	if code != http.StatusCreated {
		t.Fatalf("create round: %d %v", code, out)
	}

	// 控制台数量非法 → 400
	code, _ = post(t, srv.URL+"/api/rounds", map[string]any{
		"id": "r2", "consoles": []string{"only"}, "valves": map[string]string{"V": "closed"},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("invalid round must be 400, got %d", code)
	}

	// 依赖未满足 → 202 waiting
	waiting := map[string]any{
		"event_id": "e-b-1", "console": "beta", "seq": 1,
		"deps": map[string]int{"alpha": 1}, "valve": "GV1",
		"expected_old": "closed", "new_state": "open",
	}
	code, out = post(t, srv.URL+"/api/rounds/r1/events", waiting)
	if code != http.StatusAccepted || out["status"] != string(causality.StatusWaiting) {
		t.Fatalf("waiting: %d %v", code, out)
	}

	// 补齐前驱 → 200 applied，且等待项被级联放行
	code, out = post(t, srv.URL+"/api/rounds/r1/events", map[string]any{
		"event_id": "e-a-1", "console": "alpha", "seq": 1,
		"deps": map[string]int{}, "valve": "GV2",
		"expected_old": "closed", "new_state": "open",
	})
	if code != http.StatusOK || out["status"] != string(causality.StatusApplied) {
		t.Fatalf("applied: %d %v", code, out)
	}
	round := out["round"].(map[string]any)
	frontier := round["frontier"].(map[string]any)
	if frontier["beta"].(float64) != 1 {
		t.Fatalf("cascade must advance beta frontier: %v", frontier)
	}

	// 重投 → 既有结论
	code, out = post(t, srv.URL+"/api/rounds/r1/events", waiting)
	if code != http.StatusOK || out["status"] != string(causality.StatusApplied) {
		t.Fatalf("replay: %d %v", code, out)
	}

	// 标识复用而载荷变化 → 409
	changed := map[string]any{
		"event_id": "e-b-1", "console": "beta", "seq": 1,
		"deps": map[string]int{}, "valve": "GV1",
		"expected_old": "open", "new_state": "closed",
	}
	code, _ = post(t, srv.URL+"/api/rounds/r1/events", changed)
	if code != http.StatusConflict {
		t.Fatalf("id reuse with changed payload must be 409, got %d", code)
	}

	// 跳号 → 400
	code, out = post(t, srv.URL+"/api/rounds/r1/events", map[string]any{
		"event_id": "e-a-9", "console": "alpha", "seq": 9,
		"deps": map[string]int{}, "valve": "GV1",
		"expected_old": "open", "new_state": "closed",
	})
	if code != http.StatusBadRequest || !strings.Contains(out["reason"].(string), "gap") {
		t.Fatalf("gap must be 400, got %d %v", code, out)
	}

	// 未知控制台 → 400
	code, _ = post(t, srv.URL+"/api/rounds/r1/events", map[string]any{
		"event_id": "e-x", "console": "ghost", "seq": 1,
		"deps": map[string]int{}, "valve": "GV1",
		"expected_old": "open", "new_state": "closed",
	})
	if code != http.StatusBadRequest {
		t.Fatalf("unknown console must be 400, got %d", code)
	}

	// 未知轮次 → 404
	code, _ = post(t, srv.URL+"/api/rounds/nope/events", map[string]any{
		"event_id": "e-x", "console": "alpha", "seq": 1,
		"deps": map[string]int{}, "valve": "GV1",
		"expected_old": "open", "new_state": "closed",
	})
	if code != http.StatusNotFound {
		t.Fatalf("unknown round must be 404, got %d", code)
	}

	// 轮次状态可观察
	resp, err := http.Get(srv.URL + "/api/rounds/r1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var view map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&view)
	if view["valves"].(map[string]any)["GV1"] != "open" {
		t.Fatalf("valve state observable via API: %v", view["valves"])
	}
}
