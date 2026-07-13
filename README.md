# gRPC 튜토리얼 (Go)

gRPC를 처음부터 단계별로 배우는 실습 프로젝트입니다.

## gRPC란?

**gRPC는 다른 서버에 있는 함수를 내 로컬 함수처럼 호출하는 기술**입니다 (RPC = Remote Procedure Call). Google이 만들었고, 마이크로서비스 간 통신에 널리 쓰입니다.

두 개의 층으로 이해하면 쉽습니다:

- **인터페이스 층** — `.proto` 파일. "어떤 함수가 있고 입출력이 어떻게 생겼는지"만 정의하는 언어 중립 계약서.
- **통신 층** — gRPC 런타임. 호출을 바이너리로 직렬화하고 HTTP/2로 실어 나른 뒤 복원하는 실행 엔진.

REST와 비교하면:

| | REST | gRPC |
|---|---|---|
| 데이터 포맷 | JSON (텍스트) | Protocol Buffers (바이너리 — 더 작고 빠름) |
| 계약 정의 | 문서/OpenAPI (선택) | `.proto` 파일 (필수 — 코드 자동 생성) |
| 전송 프로토콜 | HTTP/1.1 | HTTP/2 (멀티플렉싱, 스트리밍 지원) |
| 호출 형태 | URL + HTTP 메서드 | 함수 호출처럼 (`client.CreateOrder(...)`) |

브라우저는 네이티브 gRPC를 직접 말하지 못하므로, 실제 주 무대는 **백엔드 서버 간 내부 통신**입니다.

## 프로젝트 구조

```
grpc-tutorial/
├── proto/
│   ├── order.proto        OrderService 계약 (order가 제공, payment도 호출용으로 사용)
│   └── payment.proto      PaymentService 계약 (payment가 제공, order가 호출용으로 사용)
├── gen/
│   ├── orderpb/           order.proto에서 생성된 코드 (수정 금지)
│   └── paymentpb/         payment.proto에서 생성된 코드 (수정 금지)
├── order-server/main.go   :50051 — OrderService 구현 + PaymentService 호출
├── payment-server/main.go :50052 — PaymentService 구현 + OrderService 호출
└── trigger/main.go        데모 시작용 CLI (아키텍처의 일부가 아니라 "발사 버튼")
```

## 사전 준비

```bash
# protobuf 컴파일러
brew install protobuf

# Go 코드 생성 플러그인 2개
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# 플러그인이 설치되는 ~/go/bin을 PATH에 추가 (~/.zshrc에 등록 권장)
export PATH="$PATH:$HOME/go/bin"
```

## 코드 생성

`.proto` 파일을 수정할 때마다 다시 실행합니다:

```bash
protoc \
  --go_out=. --go_opt=module=grpc-tutorial \
  --go-grpc_out=. --go-grpc_opt=module=grpc-tutorial \
  proto/order.proto proto/payment.proto
```

## 실행 방법

터미널 3개를 열고:

```bash
go run ./order-server     # 터미널 1: 주문 서버 :50051
go run ./payment-server   # 터미널 2: 결제 서버 :50052
go run ./trigger          # 터미널 3: 주문 한 건 발사 (여러 번 실행하면 ORD-002, 003...)
```

성공하면 trigger에 `주문 결과: id=ORD-001, status=PAID`가 출력되고, 두 서버 로그에서 상호 호출 과정을 볼 수 있습니다.

> 참고: 이 예제에는 **중첩 호출**이 있습니다 (order가 payment를 기다리는 동안 payment가 order를 되부름). 데드락이 안 나는 이유는 gRPC가 요청마다 별도 goroutine으로 처리하기 때문이며, 그래서 공유되는 `orders` 맵에 `sync.Mutex`를 걸었습니다.

---

## 학습 로드맵

### ✅ 1단계: Unary RPC + 서버 간 상호 호출 (완료)

가장 기본 형태 — **요청 1개를 보내면 응답 1개가 돌아옵니다.** REST API 호출과 가장 비슷한 모델입니다.

배운 것:

- **`.proto` 계약서**: `service`(호출 가능한 rpc 목록), `message`(데이터 구조). 필드 뒤 숫자(`string name = 1;`)는 값이 아니라 바이너리 직렬화용 필드 번호이며 한번 정하면 바꾸면 안 됩니다.
- **코드 생성**: `protoc`가 메시지 구조체와 통신 코드(서버 인터페이스 + 클라이언트 스텁)를 자동 생성. 생성 코드는 직접 수정하지 않습니다.
- **server/client는 역할**: 한 프로세스가 서버이자 클라이언트가 될 수 있고, 실무 마이크로서비스가 그렇습니다.
- **호출 가능 함수의 두 관문**: proto `service` 선언 + `RegisterXxxServer` 등록. 둘 다 통과해야 외부에서 호출됩니다.
- **관례**: 모든 호출에 `context.WithTimeout`으로 데드라인을 걸고, 데드라인은 하위 호출로 전파됩니다. 로컬은 `insecure`, 운영은 TLS 필수.

### ⬜ 2단계: Server Streaming

요청 1개 → 응답 여러 개. 예: 실시간 알림, 대량 데이터 조회.

### ⬜ 3단계: Client Streaming

요청 여러 개 → 응답 1개. 예: 파일 업로드, 로그 수집.

### ⬜ 4단계: Bidirectional Streaming

하나의 호출 안에서 양쪽이 스트림으로 주고받음. 예: 채팅, 실시간 협업.

### ⬜ 5단계: 실무 필수 요소

에러 처리(status code), 데드라인 전파, 메타데이터, 인터셉터(인증·로깅 미들웨어).
