# =======================================================
# Stage 1 - Build/compile app using container
# =======================================================
FROM golang:alpine AS build
ENV GOPROXY https://goproxy.cn
WORKDIR /src

RUN apk --no-cache add tzdata make
ADD go.mod go.mod
RUN go mod download
ADD . /src
RUN go build


# =======================================================
# Stage 2 - Assemble runtime image from previous stage
# =======================================================
FROM scratch
COPY --from=build /src/chain /main

COPY --from=build /usr/share/zoneinfo/Asia/Shanghai /usr/share/zoneinfo/Asia/Shanghai
ENV TZ=Asia/Shanghai

EXPOSE 30333

# 端口可自定义
ENTRYPOINT ["/main"]
