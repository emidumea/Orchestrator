package middleware

import (
	"log"
	"net/http"
	"strings"
)

func Auth(expectedToken string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")

		if !strings.HasPrefix(authHeader, "Bearer") {
			log.Printf("[Security] Access denied from %s on %s %s", r.RemoteAddr, r.Method, r.URL.Path)
			http.Error(w, "Unauthorized: Missing bearer token or invalid format", http.StatusUnauthorized)
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token != expectedToken {
			log.Printf("[Security] Access denied (invalid token) from %s", r.RemoteAddr)
			http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
