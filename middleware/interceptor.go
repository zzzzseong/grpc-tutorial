// middleware — 두 서버가 공유하는 gRPC 인터셉터.
//
// 인터셉터 = 모든 rpc 호출의 앞뒤에 끼어드는 미들웨어입니다.
// 웹 프레임워크의 filter/middleware와 같은 개념으로,
// 로깅·인증·모니터링처럼 "모든 호출에 공통인 일"을 한곳에 모읍니다.
package middleware

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// UnaryLogging 모든 unary 호출을 로깅하는 서버 인터셉터를 만듭니다.
//
// handler(ctx, req)가 실제 rpc 구현(CreateOrder 등)이고,
// 그 앞뒤로 원하는 코드를 끼워 넣는 구조입니다.
// (스트리밍 rpc용은 grpc.StreamInterceptor로 따로 등록합니다)
func UnaryLogging(server string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// 메타데이터에서 요청 추적용 ID를 꺼냅니다. (없으면 "-")
		// 메타데이터 = 본문(message) 밖에 실려 오는 key-value. HTTP 헤더와 같은 역할.
		reqID := "-"
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get("x-request-id"); len(v) > 0 {
				reqID = v[0]
			}
		}

		start := time.Now()
		resp, err := handler(ctx, req) // ← 실제 rpc 구현 실행

		// status.Code(err): 에러에서 gRPC 상태 코드를 꺼냅니다. nil이면 OK.
		log.Printf("[%s] (interceptor) %s | req-id=%s | %v | code=%s",
			server, info.FullMethod, reqID, time.Since(start).Round(time.Millisecond), status.Code(err))
		return resp, err
	}
}
