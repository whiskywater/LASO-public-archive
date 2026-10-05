FROM debian:13 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build libpq-dev libpqxx-dev libyaml-cpp-dev \
    nlohmann-json3-dev libspdlog-dev libcli11-dev libboost-system-dev \
    libgtest-dev curl jq ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug \
    && cmake --build build --parallel 2
FROM build AS validation
CMD ["ctest", "--test-dir", "build", "--output-on-failure"]
FROM debian:13-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpqxx-dev libpq5 libyaml-cpp0.8 libspdlog1.15 libboost-system1.83.0 curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system laso && useradd --system --gid laso --home-dir /var/lib/laso laso \
    && install -d -o laso -g laso /var/lib/laso /etc/laso
COPY --from=build /src/build/bin/laso /src/build/bin/laso-server /usr/local/bin/
COPY config/laso.example.yaml /etc/laso/laso.yaml
ENV LASO_DATA_DIR=/var/lib/laso LASO_CONFIG=/etc/laso/laso.yaml LASO_JSON_LOGS=true
# Supply LASO_POSTGRES_DSN at deployment time; no database credential is baked in.
USER laso:laso
WORKDIR /var/lib/laso
EXPOSE 8080
VOLUME ["/var/lib/laso"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s CMD curl -fsS "http://127.0.0.1:${LASO_API_PORT:-8080}/api/v1/health" || exit 1
ENTRYPOINT ["laso-server"]
