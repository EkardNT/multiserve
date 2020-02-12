use std::cell::Cell;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::rc::Rc;

use multiserve;

fn main() {
    multiserve::builder()
        .thread_limit(2)
        .tokio()
        // .build(|ctx| {
        //     let thread_index = ctx.thread_index();
        //     let count = Rc::new(Cell::new(0));
        //     tokio::task::spawn_local({
        //         let count = count.clone();
        //         async move {
        //             println!("Hello A: {}", thread_index);
        //             count.set(count.get() + 1);
        //         }
        //     });
        //     tokio::task::spawn_local({
        //         let count = count.clone();
        //         async move {
        //             println!("Hello B: {}", thread_index);
        //             count.set(count.get() + 1);
        //         }
        //     });
        // })
        .tcp_listener(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080))
        .build(|mut ctx| {
            println!("Hello from listener thread {} listening on {:?}", ctx.thread_index(), ctx.mut_listener().local_addr());
            let mut listener = tokio::net::TcpListener::from_std(ctx.into_listener()).unwrap();
            tokio::task::spawn_local(async move {
                loop {
                    let _ = listener.accept().await;
                    println!("Got connection!");
                }
            });
        })
        .unwrap()
        .join()
        .unwrap();
}