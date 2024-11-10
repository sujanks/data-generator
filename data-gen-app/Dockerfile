FROM golang:alpine

WORKDIR /app

COPY . .

RUN GOOS=linux GOARCH=amd64 go build -o generate .

CMD [./generate]