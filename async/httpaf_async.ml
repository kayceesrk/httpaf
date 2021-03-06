open Core.Std
open Async.Std


let read fd buffer success =
  let badfd fd = failwithf "read got back fd: %s" (Fd.to_string fd) () in
  let rec finish fd buffer result =
    let open Unix.Error in
    match result with
    | `Already_closed | `Ok 0 -> success `Eof
    | `Ok n                   -> success (`Ok n)
    | `Error (Unix.Unix_error ((EWOULDBLOCK | EAGAIN), _, _)) ->
      begin Fd.ready_to fd `Read
      >>= function
        | `Bad_fd -> badfd fd
        | `Closed -> success `Eof
        | `Ready  -> go fd buffer
      end
    | `Error (Unix.Unix_error (EBADF, _, _)) ->
      badfd fd
    | `Error exn ->
      Deferred.don't_wait_for (Fd.close fd);
      raise exn
  and go fd buffer  =
    if Fd.supports_nonblock fd then
      finish fd buffer
        (Fd.syscall fd ~nonblocking:true
          (fun file_descr ->
            Unix.Syscall_result.Int.ok_or_unix_error_exn ~syscall_name:"read"
              (Bigstring.read_assume_fd_is_nonblocking file_descr buffer)))
    else
      Fd.syscall_in_thread fd ~name:"read"
        (fun file_descr -> Bigstring.read file_descr buffer)
      >>= fun result -> finish fd buffer result
  in
  go fd buffer


open Httpaf

let create_connection_handler ?config ~request_handler =
  fun client_addr socket ->
    let fd     = Socket.fd socket in
    let writev = Faraday_async.writev_of_fd fd in
    let conn =
      Connection.create ?config (fun request request_body ->
        request_handler client_addr request request_body)
    in
    let shutdown_and_log_error = function
      | Ok ()     -> ()
      | Error exn ->
        Connection.shutdown conn;
        Socket.shutdown socket `Both;
        Log.Global.error "%s" (Exn.to_string exn);
    in
    let rec reader_thread () =
      match Connection.next_read_operation conn with
      | `Read buffer ->
        (* Log.Global.printf "read(%d)%!" (Fd.to_int_exn fd); *)
        read fd buffer (fun result ->
          Connection.report_read_result conn result;
          reader_thread ())
      | `Yield  ->
        (* Log.Global.printf "read_yield(%d)%!" (Fd.to_int_exn fd); *)
        let ivar = Ivar.create () in
        Connection.yield_reader conn (fun () -> Ivar.fill ivar ());
        Ivar.read ivar >>= reader_thread
      | `Close _ ->
        (* Log.Global.printf "read_close(%d)%!" (Fd.to_int_exn fd); *)
        Socket.shutdown socket `Receive; Deferred.unit
    in
    let rec writer_thread () =
      match Connection.next_write_operation conn with
      | `Write iovecs ->
        (* Log.Global.printf "write(%d)%!" (Fd.to_int_exn fd); *)
        writev iovecs >>= fun result ->
        Connection.report_write_result conn result;
        writer_thread ()
      | `Yield ->
        (* Log.Global.printf "write_yield(%d)%!" (Fd.to_int_exn fd); *)
        let ivar = Ivar.create () in
        Connection.yield_writer conn (fun () -> Ivar.fill ivar ());
        Ivar.read ivar >>= writer_thread
      | `Close _ ->
        (* Log.Global.printf "write_close(%d)%!" (Fd.to_int_exn fd); *)
        Socket.shutdown socket `Send; Deferred.unit
    in
    Deferred.all_ignore
      [ Monitor.try_with reader_thread >>| shutdown_and_log_error
      ; Monitor.try_with writer_thread >>| shutdown_and_log_error ]
