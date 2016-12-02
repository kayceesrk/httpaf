open Core.Std
open Async.Std


let read_of_fd fd =
  let badfd =
    failwithf "read_of_fd got bad fd: %s" (Fd.to_string fd)
  in
  let finish result =
    let open Unix.Error in
    match result with
    | `Already_closed | `Ok 0 -> return `Eof
    | `Ok n                   -> return (`Ok n)
    | `Error (Unix.Unix_error ((EWOULDBLOCK | EAGAIN), _, _)) ->
      begin Fd.ready_to fd `Read
      >>| function
        | `Bad_fd -> badfd ()
        | `Closed -> `Eof
        | `Ready  -> `Ok 0
      end
    | `Error (Unix.Unix_error (EBADF, _, _)) ->
      badfd ()
    | `Error exn ->
      Deferred.don't_wait_for (Fd.close fd);
      raise exn
  in
  let go buffer =
    if Fd.supports_nonblock fd then
      finish
        (Fd.syscall fd ~nonblocking:true
          (fun file_descr ->
            Unix.Syscall_result.Int.ok_or_unix_error_exn ~syscall_name:"read"
              (Bigstring.read_assume_fd_is_nonblocking file_descr buffer)))
    else
      Fd.syscall_in_thread fd ~name:"read"
        (fun file_descr -> Bigstring.read file_descr buffer)
      >>= finish
  in
  go

open Httpaf

let create_connection_handler ?config ~request_handler =
  fun client_addr socket ->
    let fd     = Socket.fd socket in
    let read   = read_of_fd fd in
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
      match Connection.next_read conn with
      | `Read(buffer, k) ->
        (* Log.Global.printf "read(%d)%!" (Fd.to_int_exn fd); *)
        read buffer >>= fun result ->
        begin match k result with
        | Ok ()     -> reader_thread ()
        | Error err -> assert false
        end
      | `Yield register  ->
        (* Log.Global.printf "read_yield(%d)%!" (Fd.to_int_exn fd); *)
        let ivar = Ivar.create () in
        register (fun () -> Ivar.fill ivar ());
        Ivar.read ivar >>= reader_thread
      | `Close _ ->
        (* Log.Global.printf "read_close(%d)%!" (Fd.to_int_exn fd); *)
        Socket.shutdown socket `Receive; Deferred.unit
    in
    let rec writer_thread () =
      match Connection.next_write conn with
      | `Write(iovecs, k) ->
        (* Log.Global.printf "write(%d)%!" (Fd.to_int_exn fd); *)
        writev iovecs >>= fun result ->
        k result;
        writer_thread ()
      | `Yield register ->
        (* Log.Global.printf "write_yield(%d)%!" (Fd.to_int_exn fd); *)
        let ivar = Ivar.create () in
        register (fun () -> Ivar.fill ivar ());
        Ivar.read ivar >>= writer_thread
      | `Close _ ->
        (* Log.Global.printf "write_close(%d)%!" (Fd.to_int_exn fd); *)
        Socket.shutdown socket `Send; Deferred.unit
    in
    Deferred.all_ignore
      [ Monitor.try_with reader_thread >>| shutdown_and_log_error
      ; Monitor.try_with writer_thread >>| shutdown_and_log_error ]
