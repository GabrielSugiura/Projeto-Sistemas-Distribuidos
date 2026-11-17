use bbs_rust::{
    format_request_log, format_response_log, now_iso, pack_request, parse_script_events,
    unpack_response, RequestMessage, ResponseMessage,
};
use std::env;
use std::io::{self, Write};
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
    mut req: RequestMessage,
) -> Option<ResponseMessage> {
    req.timestamp = now_iso();
    println!("{}", format_request_log(bot_name, "ENVIADO -> SERVIDOR", &req));
    let _ = io::stdout().flush();

    let packed = match pack_request(&req) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("[{}] [{}] Erro ao serializar MessagePack: {}", now_iso(), bot_name, e);
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
                println!("{}", format_response_log(bot_name, "RECEBIDO <- SERVIDOR", &rep));
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

fn main() {
    let bot_name = env::var("BOT_NAME").unwrap_or_else(|_| "bot_rs".to_string());
    let bot_index: usize = env::var("BOT_INDEX")
        .unwrap_or_else(|_| "2".to_string())
        .parse()
        .unwrap_or(2);
    let total_bots: usize = env::var("TOTAL_BOTS")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .unwrap_or(4);
    let broker_frontend =
        env::var("BROKER_FRONTEND").unwrap_or_else(|_| "tcp://broker:5555".to_string());
    let target_channels_env =
        env::var("TARGET_CHANNELS").unwrap_or_else(|_| "geral,hacking,rust-lang".to_string());
    let messages_file =
        env::var("MESSAGES_FILE").unwrap_or_else(|_| "/app/messages.txt".to_string());
    let message_interval_secs: f64 = env::var("MESSAGE_INTERVAL")
        .unwrap_or_else(|_| "2.0".to_string())
        .parse()
        .unwrap_or(2.0);
    let startup_delay_secs: f64 = env::var("STARTUP_DELAY")
        .unwrap_or_else(|_| "4.0".to_string())
        .parse()
        .unwrap_or(4.0);

    let interval = Duration::from_secs_f64(message_interval_secs);

    let target_channels: Vec<String> = target_channels_env
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    thread::sleep(Duration::from_secs_f64(startup_delay_secs));

    let ctx = zmq::Context::new();
    let mut socket = configure_req_socket(&ctx, &broker_frontend);

    println!(
        "[{}] [{}] Conectado ao Broker em {}",
        now_iso(),
        bot_name,
        broker_frontend
    );
    let _ = io::stdout().flush();

    // 1. Login do próprio bot (repete até obter sucesso)
    let mut logged_in = false;
    while !logged_in {
        let req = RequestMessage {
            action: "login".to_string(),
            user: Some(bot_name.clone()),
            channel: None,
            content: None,
            timestamp: String::new(),
        };

        if let Some(rep) = send_and_recv(&ctx, &mut socket, &broker_frontend, &bot_name, req) {
            if rep.status == "ok" {
                logged_in = true;
                continue;
            }
        }
        thread::sleep(interval);
    }

    thread::sleep(interval);

    // 2. Listar canais disponíveis
    let mut existing_channels: Vec<String> = Vec::new();
    let list_req = RequestMessage {
        action: "list_channels".to_string(),
        user: Some(bot_name.clone()),
        channel: None,
        content: None,
        timestamp: String::new(),
    };
    if let Some(rep) = send_and_recv(&ctx, &mut socket, &broker_frontend, &bot_name, list_req) {
        if rep.status == "ok" {
            if let Some(chs) = rep.channels {
                existing_channels = chs;
            }
        }
    }

    thread::sleep(interval);

    // 3. Criar canais iniciais que ainda não existem
    for channel in &target_channels {
        if existing_channels.contains(channel) && channel != "geral" {
            continue;
        }

        let create_req = RequestMessage {
            action: "create_channel".to_string(),
            user: Some(bot_name.clone()),
            channel: Some(channel.clone()),
            content: None,
            timestamp: String::new(),
        };
        let _ = send_and_recv(&ctx, &mut socket, &broker_frontend, &bot_name, create_req);
        thread::sleep(interval);
    }

    // 4. Simular mensagens e eventos do arquivo messages.txt com intervalo de 2s
    let script_events = parse_script_events(&messages_file);
    let my_events: Vec<_> = script_events
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| idx % total_bots == bot_index)
        .map(|(_, ev)| ev)
        .collect();

    println!(
        "[{}] [{}] Iniciando simulação de {} mensagens/eventos de {} (intervalo: {}s)...",
        now_iso(),
        bot_name,
        my_events.len(),
        messages_file,
        message_interval_secs
    );
    let _ = io::stdout().flush();

    for ev in my_events {
        let req = match ev.event_type.as_str() {
            "login" => RequestMessage {
                action: "login".to_string(),
                user: Some(ev.user),
                channel: None,
                content: None,
                timestamp: String::new(),
            },
            "create_channel" => RequestMessage {
                action: "create_channel".to_string(),
                user: Some(ev.user),
                channel: ev.channel,
                content: None,
                timestamp: String::new(),
            },
            _ => RequestMessage {
                action: "post_message".to_string(),
                user: Some(ev.user),
                channel: ev.channel,
                content: ev.content,
                timestamp: String::new(),
            },
        };

        let _ = send_and_recv(&ctx, &mut socket, &broker_frontend, &bot_name, req);
        thread::sleep(interval);
    }

    // 5. Listagem final de canais
    let final_list_req = RequestMessage {
        action: "list_channels".to_string(),
        user: Some(bot_name.clone()),
        channel: None,
        content: None,
        timestamp: String::new(),
    };
    let _ = send_and_recv(&ctx, &mut socket, &broker_frontend, &bot_name, final_list_req);

    println!(
        "[{}] [{}] Simulação concluída com sucesso.",
        now_iso(),
        bot_name
    );
    let _ = io::stdout().flush();

    loop {
        thread::sleep(Duration::from_secs(30));
    }
}
