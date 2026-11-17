use bbs_rust::{
    format_request_log, format_response_log, format_sub_received_log, load_sample_messages,
    now_iso, pack_request, unpack_pubsub, unpack_response, LogicalClock, ReceivedSubDisplay,
    RequestMessage, ResponseMessage,
};
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::HashSet;
use std::env;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn configure_req_socket(ctx: &zmq::Context, endpoint: &str) -> zmq::Socket {
    let socket = ctx.socket(zmq::REQ).expect("Falha ao criar socket REQ");
    socket.set_rcvtimeo(5000).expect("Falha ao definir RCVTIMEO");
    socket.set_sndtimeo(5000).expect("Falha ao definir SNDTIMEO");
    socket.set_linger(0).expect("Falha ao definir LINGER");
    socket
        .connect(endpoint)
        .expect("Falha ao conectar ao frontend do Broker");
    socket
}

fn send_and_recv(
    ctx: &zmq::Context,
    socket: &mut zmq::Socket,
    endpoint: &str,
    bot_name: &str,
    clock: &LogicalClock,
    mut req: RequestMessage,
) -> Option<ResponseMessage> {
    req.timestamp = now_iso();
    req.logical_clock = clock.tick_send();
    let out_dir = format!("ENVIADO (REQ) -> SERVIDOR [clock={}]", req.logical_clock);
    println!("{}", format_request_log(bot_name, &out_dir, &req));
    let _ = io::stdout().flush();

    let packed = match pack_request(&req) {
        Ok(b) => b,
        Err(e) => {
            eprintln!(
                "[{}] [{}] Erro ao serializar MessagePack: {}",
                now_iso(),
                bot_name,
                e
            );
            return None;
        }
    };

    if let Err(e) = socket.send(packed, 0) {
        println!(
            "[{}] [{}] Timeout/Erro ZMQ ao enviar ({}). Reconectando socket...",
            now_iso(),
            bot_name,
            e
        );
        let _ = io::stdout().flush();
        *socket = configure_req_socket(ctx, endpoint);
        return None;
    }

    match socket.recv_bytes(0) {
        Ok(raw_rep) => match unpack_response(&raw_rep) {
            Ok(rep) => {
                let updated_clk = clock.update_receive(rep.logical_clock);
                let in_dir = format!("RECEBIDO (REP) <- SERVIDOR [clock={}]", updated_clk);
                println!("{}", format_response_log(bot_name, &in_dir, &rep));
                let _ = io::stdout().flush();
                Some(rep)
            }
            Err(e) => {
                eprintln!(
                    "[{}] [{}] Erro ao desserializar resposta MessagePack: {}",
                    now_iso(),
                    bot_name,
                    e
                );
                None
            }
        },
        Err(e) => {
            println!(
                "[{}] [{}] Timeout/Erro ZMQ ao receber ({}). Reconectando socket...",
                now_iso(),
                bot_name,
                e
            );
            let _ = io::stdout().flush();
            *socket = configure_req_socket(ctx, endpoint);
            None
        }
    }
}

fn subscriber_worker(
    ctx: zmq::Context,
    pubsub_xpub: String,
    bot_name: String,
    clock: Arc<LogicalClock>,
    subscribed_channels: Arc<Mutex<HashSet<String>>>,
) {
    let sub_socket = ctx.socket(zmq::SUB).expect("Falha ao criar socket SUB");
    sub_socket
        .set_rcvtimeo(500)
        .expect("Falha ao definir RCVTIMEO no SUB");
    sub_socket.set_linger(0).expect("Falha ao definir LINGER");
    sub_socket
        .connect(&pubsub_xpub)
        .expect("Falha ao conectar ao XPUB do proxy Pub/Sub");

    let mut applied_topics: HashSet<String> = HashSet::new();

    loop {
        let pending: Vec<String> = {
            let guard = subscribed_channels.lock().unwrap();
            guard.difference(&applied_topics).cloned().collect()
        };

        for topic in pending {
            if sub_socket.set_subscribe(topic.as_bytes()).is_ok() {
                applied_topics.insert(topic.clone());
                println!(
                    "[{}] [{}] [SUB] Inscrito no tópico/canal '#{}' em {}",
                    now_iso(),
                    bot_name,
                    topic,
                    pubsub_xpub
                );
                let _ = io::stdout().flush();
            }
        }

        match sub_socket.recv_multipart(0) {
            Ok(parts) => {
                let received_at = now_iso();
                if parts.len() >= 2 {
                    let topic = String::from_utf8_lossy(&parts[0]).to_string();
                    if let Ok(msg_data) = unpack_pubsub(&parts[1]) {
                        let msg_clock = msg_data.logical_clock;
                        let updated_clk = clock.update_receive(msg_clock);
                        let display = ReceivedSubDisplay {
                            canal: topic,
                            autor: msg_data.user,
                            mensagem: msg_data.content,
                            servidor_pub: msg_data.server_id,
                            timestamp_envio: msg_data.timestamp,
                            timestamp_recebimento: received_at,
                            clock_recebido: msg_clock,
                            clock_local_atual: updated_clk,
                        };
                        println!("{}", format_sub_received_log(&bot_name, &display));
                        let _ = io::stdout().flush();
                    }
                }
            }
            Err(zmq::Error::EAGAIN) => continue,
            Err(err) => {
                eprintln!("[{}] [{}] [SUB] Erro ZMQ: {}", now_iso(), bot_name, err);
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

fn main() {
    let bot_name = env::var("BOT_NAME").unwrap_or_else(|_| "bot_rs".to_string());
    let broker_frontend =
        env::var("BROKER_FRONTEND").unwrap_or_else(|_| "tcp://broker:5555".to_string());
    let pubsub_xpub =
        env::var("PUBSUB_XPUB").unwrap_or_else(|_| "tcp://pubsub-proxy:5558".to_string());
    let candidates_env = env::var("CANDIDATE_CHANNELS").unwrap_or_else(|_| {
        "geral,hacking,rust-lang,zeromq-libzmq,sistemas-distribuidos,avisos,random".to_string()
    });
    let messages_file =
        env::var("MESSAGES_FILE").unwrap_or_else(|_| "/app/messages.txt".to_string());
    let message_interval_secs: f64 = env::var("MESSAGE_INTERVAL")
        .unwrap_or_else(|_| "1.0".to_string())
        .parse()
        .unwrap_or(1.0);
    let startup_delay_secs: f64 = env::var("STARTUP_DELAY")
        .unwrap_or_else(|_| "4.0".to_string())
        .parse()
        .unwrap_or(4.0);

    let interval = Duration::from_secs_f64(message_interval_secs);

    let candidate_channels: Vec<String> = candidates_env
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let sample_messages = load_sample_messages(&messages_file);

    thread::sleep(Duration::from_secs_f64(startup_delay_secs));

    let clock = Arc::new(LogicalClock::new(0));
    let ctx = zmq::Context::new();
    let mut req_socket = configure_req_socket(&ctx, &broker_frontend);

    let subscribed_channels: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let sub_ctx = ctx.clone();
    let sub_endpoint = pubsub_xpub.clone();
    let sub_bot = bot_name.clone();
    let sub_clock = Arc::clone(&clock);
    let sub_set = Arc::clone(&subscribed_channels);

    thread::spawn(move || {
        subscriber_worker(sub_ctx, sub_endpoint, sub_bot, sub_clock, sub_set);
    });

    println!(
        "[{}] [{}] Conectado | Broker REQ: {} | Proxy SUB: {} | clock={}",
        now_iso(),
        bot_name,
        broker_frontend,
        pubsub_xpub,
        clock.value()
    );
    let _ = io::stdout().flush();

    let mut logged_in = false;
    while !logged_in {
        let req = RequestMessage {
            action: "login".to_string(),
            user: Some(bot_name.clone()),
            channel: None,
            content: None,
            limit: None,
            timestamp: String::new(),
            logical_clock: 0,
        };

        if let Some(rep) =
            send_and_recv(&ctx, &mut req_socket, &broker_frontend, &bot_name, &clock, req)
        {
            if rep.status == "ok" {
                logged_in = true;
                continue;
            }
        }
        thread::sleep(Duration::from_secs(1));
    }

    let mut rng = rand::thread_rng();
    let mut cycle: u64 = 0;

    loop {
        cycle += 1;

        let mut available_channels: Vec<String> = Vec::new();
        let list_req = RequestMessage {
            action: "list_channels".to_string(),
            user: Some(bot_name.clone()),
            channel: None,
            content: None,
            limit: None,
            timestamp: String::new(),
            logical_clock: 0,
        };
        if let Some(rep) = send_and_recv(
            &ctx,
            &mut req_socket,
            &broker_frontend,
            &bot_name,
            &clock,
            list_req,
        ) {
            if rep.status == "ok" {
                if let Some(chs) = rep.channels {
                    available_channels = chs;
                }
            }
        }

        if available_channels.len() < 5 {
            let missing: Vec<String> = candidate_channels
                .iter()
                .filter(|c| !available_channels.contains(c))
                .cloned()
                .collect();

            let new_channel = if let Some(chosen) = missing.choose(&mut rng) {
                chosen.clone()
            } else {
                format!("canal-{}-{}", bot_name, rng.gen_range(100..999))
            };

            let create_req = RequestMessage {
                action: "create_channel".to_string(),
                user: Some(bot_name.clone()),
                channel: Some(new_channel.clone()),
                content: None,
                limit: None,
                timestamp: String::new(),
                logical_clock: 0,
            };
            if let Some(rep) = send_and_recv(
                &ctx,
                &mut req_socket,
                &broker_frontend,
                &bot_name,
                &clock,
                create_req,
            ) {
                if rep.status == "ok" {
                    available_channels.push(new_channel);
                }
            }
        }

        let chosen_sub: Option<String> = {
            let guard = subscribed_channels.lock().unwrap();
            if guard.len() < 3 {
                let unsubscribed: Vec<String> = available_channels
                    .iter()
                    .filter(|c| !guard.contains(*c))
                    .cloned()
                    .collect();
                unsubscribed.choose(&mut rng).cloned()
            } else {
                None
            }
        };

        if let Some(topic) = chosen_sub {
            {
                let mut guard = subscribed_channels.lock().unwrap();
                guard.insert(topic);
            }
            thread::sleep(Duration::from_millis(300));
        }

        if available_channels.is_empty() {
            available_channels.push("geral".to_string());
        }

        let target_channel = available_channels
            .choose(&mut rng)
            .cloned()
            .unwrap_or_else(|| "geral".to_string());

        println!(
            "[{}] [{}] Ciclo #{}: publicando 10 mensagens em '#{}' (intervalo: {}s)...",
            now_iso(),
            bot_name,
            cycle,
            target_channel,
            message_interval_secs
        );
        let _ = io::stdout().flush();

        for msg_idx in 1..=10 {
            let base_text = sample_messages
                .choose(&mut rng)
                .cloned()
                .unwrap_or_else(|| "Mensagem de teste em Rust".to_string());
            let content = format!("({}/10) {}", msg_idx, base_text);

            let pub_req = RequestMessage {
                action: "publish".to_string(),
                user: Some(bot_name.clone()),
                channel: Some(target_channel.clone()),
                content: Some(content),
                limit: None,
                timestamp: String::new(),
                logical_clock: 0,
            };

            let _ = send_and_recv(
                &ctx,
                &mut req_socket,
                &broker_frontend,
                &bot_name,
                &clock,
                pub_req,
            );
            thread::sleep(interval);
        }

        let hist_req = RequestMessage {
            action: "get_history".to_string(),
            user: Some(bot_name.clone()),
            channel: None,
            content: None,
            limit: Some(20),
            timestamp: String::new(),
            logical_clock: 0,
        };
        if let Some(rep_hist) = send_and_recv(
            &ctx,
            &mut req_socket,
            &broker_frontend,
            &bot_name,
            &clock,
            hist_req,
        ) {
            if rep_hist.status == "ok" {
                println!(
                    "[{}] [{}] [HISTÓRICO REPLICADO] Servidor '{}' possui {:?} mensagens totais replicadas.",
                    now_iso(),
                    bot_name,
                    rep_hist.server_id,
                    rep_hist.total_messages
                );
                let _ = io::stdout().flush();
            }
        }
    }
}