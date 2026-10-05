package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/xjeey8iust/sen-privacy-job-registry/internal/store"
)

// storageUnavailableMessage is the single message every storage failure surfaces,
// so /healthz and the job entries stay consistent.
const storageUnavailableMessage = "database is not available"

// NewRouter wires the public HTTP surface. The service contract in README.md describes
// the error shape every entry must keep.
func NewRouter(st *store.Store) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/healthz", func(c *gin.Context) {
		if err := st.Ping(); err != nil {
			writeError(c, http.StatusServiceUnavailable, "storage_unavailable", storageUnavailableMessage)
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "database": "ok"})
	})

	router.POST("/jobs", func(c *gin.Context) { registerJob(c, st) })
	router.GET("/jobs", func(c *gin.Context) { listJobs(c, st) })

	router.NoRoute(func(c *gin.Context) {
		writeError(c, http.StatusNotFound, "route_not_found", "no route matches this path")
	})
	return router
}

// writeError emits the single top-level error object every failure shares.
func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}
