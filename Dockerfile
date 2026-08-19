# syntax=docker/dockerfile:1

# Stage 1: build the React frontend.
FROM node:22-alpine AS web
WORKDIR /app/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ .
RUN npm run build

# Stage 2: build the Go binary. The frontend dist/ is already present under
# internal/web/dist, which //go:embed requires.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /app/internal/web/dist ./internal/web/dist
RUN CGO_ENABLED=0 go build -o /out/event-pipeline ./cmd/event-pipeline

# Stage 3: minimal runtime. Redis connections are internal, so no CA bundle
# is needed; scratch keeps the image small.
FROM scratch
COPY --from=build /out/event-pipeline /event-pipeline
EXPOSE 8080
ENTRYPOINT ["/event-pipeline"]
