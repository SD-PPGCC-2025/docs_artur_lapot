# Usar uma imagem base do Rust
FROM rust:1.75 AS builder

# Definir diretório de trabalho
WORKDIR /usr/src/app

# Copiar arquivos de configuração primeiro (para cache de dependências)
COPY Cargo.toml ./

# Criar um arquivo src/main.rs vazio para compilar dependências
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Compilar dependências (será cached se Cargo.toml não mudar)
RUN cargo build --release
RUN rm src/main.rs

# Copiar código fonte
COPY src ./src

# Compilar aplicação
RUN cargo build --release

# Imagem final (menor)
FROM debian:bookworm-slim

# Instalar dependências necessárias
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Criar usuário não-root
RUN useradd -m -u 1001 membership

# Copiar binário da imagem builder
COPY --from=builder /usr/src/app/target/release/algoritmo_membership /usr/local/bin/membership

# Mudar para usuário não-root
USER membership

# Definir diretório de trabalho
WORKDIR /home/membership

# Expor porta padrão (pode ser alterada via variáveis de ambiente)
EXPOSE 8001

# Comando padrão
ENTRYPOINT ["membership"]
CMD ["node1", "8001"]
