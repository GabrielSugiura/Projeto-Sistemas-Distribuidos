# Sistema Distribuído de Troca de Mensagens (BBS / IRC)

Protótipo distribuído inspirado em sistemas BBS/IRC desenvolvido em **Python** e **Rust** utilizando **ZeroMQ** e orquestrado via **Docker Compose**.

---

## 1. Arquitetura e Componentes

O sistema executa **4 clientes (bots)** e **4 servidores** em duas linguagens distintas, garantindo total interoperabilidade entre todos os processos:

* **Broker `REQ/REP` (`python/broker.py`):** Proxy `ROUTER` (`5555`) / `DEALER` (`5556`) que balanceia as requisições dos clientes entre os servidores via *round-robin*.
* **Proxy `PUB/SUB` (`python/pubsub_proxy.py`):** Proxy `XSUB` (`5557`) / `XPUB` (`5558`) para difusão de mensagens nos canais, anúncios de eleição (`servers`) e replicação entre servidores (`replication`).
* **Serviço de Referência (`python/reference.py`):** Atribui *ranks* únicos aos servidores (`5559`) e monitora disponibilidade via *heartbeat* a cada 10 mensagens recebidas.
* **Servidores (`python/server.py` e `rust/src/bin/server.rs`):** 2 instâncias Python e 2 instâncias Rust com persistência isolada em disco (`/data/storage.msgpack`), canal peer-to-peer (`5560`), eleição de coordenador, sincronização de relógio físico (Algoritmo de Berkeley a cada 15 mensagens) e replicação de dados.
* **Clientes / Bots (`python/client.py` e `rust/src/bin/client.rs`):** 2 bots Python e 2 bots Rust que realizam *login*, criam canais (quando há menos de 5), inscrevem-se em até 3 canais aleatórios e publicam mensagens continuamente.

---

## 2. Protocolo, Relógios e Persistência

* **Serialização Binária:** Todas as mensagens na rede utilizam **MessagePack** (`msgpack` em Python e `rmp-serde` em Rust) com chaves nomeadas.
* **Relógios:** Toda mensagem carrega o `timestamp` físico (ISO-8601 UTC ajustado via Berkeley nos servidores) e o contador `logical_clock` (**Relógio Lógico de Lamport**, incrementado antes de cada envio e atualizado com `max(local, recebido)` no recebimento).
* **Armazenamento:** Cada servidor mantém seu próprio arquivo binário `/data/storage.msgpack` (sem compartilhamento de volume entre servidores), gravando logins, canais e publicações.

---

## 3. Consistência e Replicação (Parte 5)

### Problema
Como o broker `REQ/REP` distribui requisições em *round-robin*, cada servidor recebe originalmente apenas uma fração das operações de escrita. Sem replicação, a falha de um servidor causa perda de histórico e leituras incompletas.

### Método Escolhido
Com base nos modelos estudados na disciplina, foi adotada a seguinte combinação:

1. **Modelo de Consistência — Consistência Eventual Forte:**
   * Para evitar o alto custo de sincronização global e travas distribuídas diante de escritas concorrentes entre os 4 servidores, o sistema relaxa a consistência imediata e garante que, cessadas as atualizações, todas as réplicas convergem para o mesmo estado (**Consistência Eventual**).
   * Como escritas concorrentes chegam em servidores distintos, aplica-se a **Consistência Eventual Forte**: conflitos de ordenação são resolvidos de forma determinística utilizando os **relógios** — todas as réplicas ordenam o histórico pela tupla `(logical_clock, published_at, server_id, op_id)`.

2. **Distribuição de Conteúdo — Réplica Ativa (*Push*) + Réplica Passiva (*Pull*):**
   * **Réplica Ativa com atualização por *push*:** Sempre que um servidor processa uma operação de escrita (`login`, `create_channel` ou `publish`), ele aplica localmente e propaga a operação (`ReplicationEvent`) pelo tópico `replication` no proxy `PUB/SUB` para todos os demais servidores.
   * **Réplica Passiva com atualização por *pull*:** Ao iniciar (logo após a eleição do coordenador) e periodicamente a cada 15 mensagens, cada servidor solicita uma cópia completa dos dados (`REQ: sync_state`) ao coordenador ou a outro par ativo na porta `5560`, recuperando atualizações perdidas enquanto esteve fora do ar.

### Adaptações Necessárias no Projeto
* **Idempotência via `op_id`:** Como uma réplica pode receber a mesma atualização tanto pelo *push* (`PUB/SUB`) quanto pelo *pull* (`sync_state`), cada escrita recebe um identificador único (`<servidor>:<logical_clock>:<timestamp>`), garantindo que operações já aplicadas sejam ignoradas sem duplicar registros.
* **Contorno ao *Slow Joiner* do ZeroMQ:** Como sockets `SUB` podem perder mensagens publicadas nos primeiros milissegundos de conexão, o *pull* inicial (`sync_state`) sincroniza o estado acumulado antes e durante a operação normal.
* **Interoperabilidade Python ↔ Rust:** Os eventos de replicação e os *snapshots* de transferência de estado seguem o mesmo esquema de mapas binários MessagePack, permitindo que servidores Python repliquem dados para servidores Rust e vice-versa.

---
