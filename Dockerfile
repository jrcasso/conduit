# syntax=docker/dockerfile:1
FROM golang:1.19

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq && \
    apt-get install -yq \
        curl \
        unzip && \
    curl https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o awscliv2.zip && \
        unzip awscliv2.zip && \
        ./aws/install && \
    apt-get clean && \
    rm -rf aws/ awscliv2.zip /var/lib/apt/lists/*
