# Sistema de Troca de Mensagens Instantâneas (Projeto Sistemas Distribuidos)

### Resumo das Funcionalidades Implementadas
* **Parte 1 (Broker `ROUTER/DEALER` + `REQ/REP` + Persistência Binária):** Balanceamento *round-robin* entre 4 servidores (2 Python, 2 Rust) atendendo 4 bots, com serialização em **MessagePack** e persistência em disco (`/data/storage.msgpack`).
* **Parte 2 (Publicação e Assinatura `PUB/SUB`):** Proxy dedicado (`XSUB:5557` / `XPUB:5558`) para difusão em tempo real nos canais criados dinamicamente.
* **Parte 3 (Relógio Lógico de Lamport + Serviço de Referência + Heartbeat):** Relógio lógico de Lamport em todas as mensagens de todos os processos e serviço `reference` (`:5559`) para atribuição de `rank`, listagem de servidores ativos e monitoramento de *heartbeat* a cada 10 mensagens.
* **Parte 4 (Eleição de Coordenador + Sincronização de Berkeley):** Eleição baseada no algoritmo do Valentão (*Bully*) com anúncio no tópico `servers`, e sincronização periódica dos relógios físicos a cada 15 mensagens trocadas via algoritmo de Berkeley.
* **Parte 5 (Consistência e Replicação Total entre Servidores):** Todos os servidores mantêm uma réplica completa e ordenadamente consistente de todos os logins, canais e mensagens publicadas no cluster.

---

## Parte 5: Método de Consistência e Replicação Escolhido

### O Problema no Projeto
Como o *broker* ZeroMQ (`ROUTER/DEALER`) distribui as requisições dos clientes entre os 4 servidores utilizando balanceamento **round-robin**, cada servidor recebe originalmente apenas uma fração (~25%) das operações de `login`, `create_channel` e `publish`. Sem replicação:
1. A falha ou reinício de um servidor provoca perda de parte do histórico global do sistema.
2. Quando um cliente solicita o histórico de mensagens (`action: "get_history"`), a resposta depende de qual servidor o *broker* escolheu naquela rodada, retornando um histórico incompleto e divergente entre réplicas.

### Método Teórico Escolhido
Para resolver o problema, adotamos a **Replicação Ativa (Máquina de Estados Replicada - *State Machine Replication*) com Ordenação Total via Relógios Lógicos de Lamport**, complementada por **Anti-Entropia (*State Transfer / Catch-up*)** para tolerância a falhas e recuperação de nós:

* **Replicação Ativa (*State Machine Replication*):** Todas as réplicas iniciam no mesmo estado determinístico (`logins`, `channels`, `publications`, `applied_ops`) e executam a mesma sequência de transições de estado determinísticas. Sempre que qualquer servidor processa uma operação de escrita de um cliente (`login`, `create_channel` ou `publish`), ele aplica a alteração localmente e difunde imediatamente um evento de replicação (`ReplicationEvent`) no tópico dedicado **`replication`** através do proxy `PUB/SUB`.
* **Ordenação Total (*Total-Order Broadcast* com Relógio de Lamport):** Como múltiplos servidores podem processar mensagens simultaneamente em paralelo, cada evento replicado carrega seu timestamp lógico de Lamport (`logical_clock`), o timestamp físico sincronizado via Berkeley (`published_at`) e a identidade do servidor de origem (`server_id` + `op_id`). Todas as réplicas ordenam o histórico de mensagens pela tupla determinística `(logical_clock, published_at, server_id, op_id)`. Dessa forma, mesmo que eventos concorrentes cheguem em ordens distintas pela rede em cada container, todas as réplicas convergem para **exatamente a mesma ordem total** de histórico (**Consistência Forte Eventual / *Strong Eventual Consistency***).
* **Anti-Entropia (*Catch-up* via Transferência de Estado):** Complementa a difusão `PUB/SUB` permitindo que qualquer servidor recém-iniciado, atrasado ou recuperado de uma falha sincronize seu estado completo diretamente com o **coordenador** (ou com outro servidor ativo) por meio da requisição peer-to-peer `REQ: sync_state`.

### Como Foi Implementado no Código
1. **Difusão em Tempo Real (`tópico: replication`):**
   * Nos servidores (`python/server.py` e `rust/src/bin/server.rs`), ao concluir `login`, `create_channel` ou `publish`, o servidor gera um identificador único `op_id = "<server_id>:<logical_clock>:<timestamp>"`, grava no `storage.msgpack` e envia `pub_socket.send_multipart([b"replication", pack(event)])`.
   * A *thread* receptora `SUB` de cada servidor assina simultaneamente os tópicos `servers` e `replication`. Ao receber um `ReplicationEvent` de outro servidor, atualiza seu relógio lógico (`clock = max(clock, event.logical_clock)`) e aplica a operação ao `DiskStorage`.
2. **Sincronização de Estado / Anti-Entropia (`action: "sync_state"` na porta `5560`):**
   * O canal peer-to-peer (`tcp://<server_id>:5560`) utilizado na Parte 4 para eleição e Berkeley foi estendido para responder à ação `sync_state`, retornando o snapshot completo (`logins`, `channels`, `publications`, `applied_ops`).
   * Cada servidor executa `sync_state_with_cluster` logo após a inicialização/eleição (eliminando perda de mensagens durante a subida dos containers) e periodicamente a cada ciclo de 15 mensagens junto à sincronização de Berkeley.
3. **Consulta de Histórico Unificado (`action: "get_history"`):**
   * Os servidores agora atendem à ação `get_history` (com filtro opcional por `channel`), retornando o histórico global replicado e ordenado. Os bots consultam `get_history` ao final de cada ciclo de 10 publicações para validar a consistência entre os servidores.

### Mudanças e Adaptações Necessárias no Método Teórico
Para adaptar a Replicação Ativa clássica à arquitetura ZeroMQ do projeto, foram necessárias 4 adaptações práticas:
1. **Idempotência explícita via `op_id`:** Como o modelo `PUB/SUB` entrega mensagens a todos os assinantes (inclusive ao próprio publicador) e a anti-entropia pode reenviar operações já recebidas, cada servidor mantém o conjunto `applied_ops` persistido em disco. Operações já aplicadas são descartadas de forma idempotente sem duplicar registros.
2. **Contorno ao problema de *Slow Joiner* do ZeroMQ `PUB/SUB`:** Sockets `PUB/SUB` no ZeroMQ não armazenam histórico para assinantes que conectam depois que a publicação ocorreu. A combinação da difusão `PUB/SUB` com o *catch-up* inicial e periódico via `REQ/REP` (`sync_state`) garante que nenhum servidor fique sem mensagens publicadas durante o boot ou após uma queda temporária.
3. **Interoperabilidade Binária Python ↔ Rust:** Como o cluster é misto (2 servidores em Python e 2 em Rust), todas as estruturas de replicação (`ReplicationEvent` e `PeerResponse` de `sync_state`) foram padronizadas como mapas nomeados em **MessagePack**, garantindo compatibilidade transparente entre as duas linguagens.
4. **Controle de Concorrência Thread-Safe no Storage:** Como o `DiskStorage` passou a ser atualizado tanto pela *thread* principal (`REQ/REP` dos clientes e anti-entropia) quanto pela *thread* assíncrona (`SUB` do tópico `replication`), todo acesso ao estado e gravação atômica em disco (`*.tmp` + `rename`) foi protegido por exclusão mútua (`threading.Lock` em Python e `Arc<Mutex<DiskStorage>>` em Rust).

---

## Como Executar e Verificar a Replicação

**Subir todos os serviços:**
   ```bash
   docker compose up --build
   ```

**Verificar que todos os servidores possuem os mesmos dados:**
   * Nos logs do Docker Compose, observe os eventos `[REPL-PUB]`, `[REPL-SUB]` e `[ANTI-ENTROPIA]`, além das respostas de `get_history` retornando o mesmo total de mensagens independentemente de qual servidor atendeu o bot.
   * Você também pode inspecionar os arquivos persistidos em `./dados/server-python-1/storage.msgpack`, `./dados/server-python-2/storage.msgpack`, `./dados/server-rust-1/storage.msgpack` e `./dados/server-rust-2/storage.msgpack`.
