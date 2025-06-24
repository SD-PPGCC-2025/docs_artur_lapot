# Algoritmo de Membership para Sistemas Distribuídos

Este projeto implementa um algoritmo de membership distribuído com detecção de falhas e protocolo de fofoca (gossip) para sistemas distribuídos, desenvolvido em Rust para apresentação acadêmica de mestrado.

## Características Principais

- **Detecção de Falhas Automatizada**: Sistema baseado em heartbeat com timeout configurável
- **Protocolo de Fofoca (Gossip)**: Disseminação eficiente e confiável de informações entre membros
- **Estados de Membro**: Alive (Vivo), Suspected (Suspeito), Failed (Falhado)
- **Membership Dinâmico**: Entrada e saída dinâmica de membros no cluster
- **Alta Disponibilidade**: Sistema tolerante a falhas parciais e partições de rede
- **Monitoramento em Tempo Real**: Visualização da tabela de membership atualizada continuamente
- **Comunicação UDP**: Protocolo leve e eficiente para troca de mensagens
- **Containerização**: Suporte completo ao Docker para facilitar demonstrações

## Como Executar

### Opção 1: Desenvolvimento Local (Rust necessário)

Para desenvolvimento e testes locais, você precisa ter o Rust instalado:

```bash
# Terminal 1 - Nó seed (primeiro nó do cluster)
cargo run -- node1 8001

# Terminal 2 - Segundo nó (conecta ao seed)
cargo run -- node2 8002 127.0.0.1:8001

# Terminal 3 - Terceiro nó (conecta ao seed)
cargo run -- node3 8003 127.0.0.1:8001

# Terminal 4 - Quarto nó (opcional)
cargo run -- node4 8004 127.0.0.1:8001
```

### Opção 2: Executável Compilado (sem Rust)

```bash
# 1. Compilar o projeto para release
cargo build --release

# 2. Executar os nós usando o executável
./target/release/membership node1 8001
./target/release/membership node2 8002 127.0.0.1:8001
./target/release/membership node3 8003 127.0.0.1:8001
```

### Opção 3: Docker (Recomendado para Demonstrações)

A opção mais simples e confiável para demonstrações:

```bash
# Construir e executar cluster completo (4 nós)
docker-compose up -d

# Visualizar logs em tempo real
docker-compose logs -f

# Visualizar logs de um nó específico
docker-compose logs -f node1

# Parar o cluster
docker-compose down

# Remover containers e imagens
docker-compose down --rmi all
```

### Opção 4: Docker Manual (para testes específicos)

```bash
# Construir imagem
docker build -t membership-algorithm .

# Executar nós manualmente
docker run --network host --name node1 membership-algorithm node1 8001
docker run --network host --name node2 membership-algorithm node2 8002 127.0.0.1:8001
```

## Arquitetura do Sistema

### Componentes Principais

1. **MembershipService**: Coordenador central que gerencia o estado do cluster
2. **Heartbeat Sender**: Transmite sinais de vida periódicos para outros membros
3. **Message Receiver**: Processa mensagens UDP recebidas (heartbeat, gossip, join/leave)
4. **Failure Detector**: Monitora e detecta falhas baseado em timeouts de heartbeat
5. **Gossip Sender**: Implementa protocolo de fofoca para disseminação de informações
6. **Member Table**: Mantém estado consistente de todos os membros conhecidos

### Protocolo de Comunicação

#### Tipos de Mensagem (UDP)

- **Heartbeat**: Sinais de vida enviados periodicamente entre membros
- **Gossip**: Compartilhamento de informações sobre estado de outros membros
- **Join**: Solicitação de entrada de novo membro no cluster
- **Leave**: Notificação de saída graceful de membro

#### Estados de Membro

- **Alive (Vivo)**: Membro ativo, respondendo normalmente
- **Suspected (Suspeito)**: Membro não responde dentro do timeout, mas ainda não declarado falho
- **Failed (Falhado)**: Membro declarado como falho após período de suspeita

### Algoritmos Implementados

1. **Detecção de Falhas**: Timeout baseado em última atividade
2. **Protocolo de Fofoca**: Disseminação probabilística de informações
3. **Convergência Eventual**: Garantia de consistência eventual da tabela de membership

## Roteiro para Apresentação Acadêmica

### Cenário 1: Formação Inicial do Cluster

```bash
# Demonstrar inicialização do cluster
docker-compose up -d

# Observar logs em tempo real
docker-compose logs -f
```

**Pontos a destacar:**

- Nó seed (node1) inicia sozinho
- Demais nós conectam automaticamente ao seed
- Tabela de membership converge rapidamente
- Protocolo de gossip dissemina informações

### Cenário 2: Detecção e Recuperação de Falhas

```bash
# Simular falha abrupta
docker stop membership-node2

# Observar detecção de falha nos logs
docker-compose logs -f node1

# Simular recuperação
docker-compose start node2
```

**Pontos a destacar:**

- Transição de estados: Alive → Suspected → Failed
- Tempo de detecção configurável
- Disseminação da falha via gossip
- Recuperação automática quando nó retorna

### Cenário 3: Escalabilidade Dinâmica

```bash
# Adicionar novos nós durante execução
docker run --network host --name node5 membership-algorithm:latest node5 8005 127.0.0.1:8001

# Observar descoberta automática
docker-compose logs -f
```

**Pontos a destacar:**

- Descoberta automática de novos membros
- Convergência eventual da tabela de membership
- Tolerância a múltiplas falhas simultâneas

### Cenário 4: Shutdown Graceful vs Falha Abrupta

```bash
# Shutdown graceful (Ctrl+C em execução local)
# vs
# Falha abrupta (docker stop)
```

**Pontos a destacar:**

- Diferença entre leave graceful e detecção de falha
- Impacto na convergência do sistema

## Configuração e Parâmetros

### Parâmetros do Sistema (configurados no código)

- **failure_timeout**: 10 segundos - Tempo para considerar membro suspeito
- **gossip_interval**: 2 segundos - Intervalo entre disseminação de gossip
- **heartbeat_interval**: 1 segundo - Intervalo entre heartbeats
- **max_retries**: 5 tentativas - Máximo de tentativas antes de declarar falha

### Argumentos de Linha de Comando

```bash
./membership <node_id> <port> [seed_address]
```

- **node_id**: Identificador único do nó (ex: node1, node2)
- **port**: Porta UDP para comunicação (ex: 8001, 8002)
- **seed_address**: Endereço do nó seed (opcional para o primeiro nó)

### Requisitos do Sistema

#### Desenvolvimento Local

- Rust 1.70+ (toolchain stable)
- Cargo (gerenciador de pacotes do Rust)
- Sistema operacional: Linux, macOS, ou Windows

#### Execução com Docker

- Docker Engine 20.10+
- Docker Compose 2.0+
- Mínimo 512MB RAM por container

## Dependências do Projeto

### Dependências Rust (Cargo.toml)

- `serde` (1.0): Serialização e deserialização de dados
- `bincode` (1.3): Codificação binária eficiente para mensagens UDP
- `ctrlc` (3.0): Tratamento de sinais do sistema (SIGINT, SIGTERM)

### Estrutura de Arquivos

```
algotirimo_membership/
├── src/
│   └── main.rs              # Código principal do algoritmo
├── Dockerfile               # Container Docker otimizado
├── docker-compose.yml       # Orquestração do cluster
├── .dockerignore           # Arquivos ignorados no build
├── Cargo.toml              # Configuração do projeto Rust
└── README.md               # Documentação (este arquivo)
```

## Resolução de Problemas

### Problemas Comuns

1. **Erro de bind de porta**: Verificar se as portas 8001-8004 estão livres
2. **Containers não se comunicam**: Usar `network_mode: host` no Docker
3. **Build falha no Docker**: Verificar versão do Docker e disponibilidade de rede

### Logs e Debug

```bash
# Ver logs detalhados
docker-compose logs -f --tail=100

# Verificar status dos containers
docker-compose ps

# Inspecionar rede
docker network ls
```

## Contribuição e Desenvolvimento

### Compilação Local

```bash
# Debug (desenvolvimento)
cargo build

# Release (produção)
cargo build --release

# Executar testes
cargo test

# Verificar código
cargo check
cargo clippy
```

## Referências Acadêmicas

- SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol
- Gossip Protocols: Design and Applications in Large-scale Distributed Systems
- Failure Detection in Asynchronous Distributed Systems

## Autor

**Artur Rocha Lapot**  
Mestrando em Ciência da Computação  
Especialização em Sistemas Distribuídos
