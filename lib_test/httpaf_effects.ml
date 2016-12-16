open Httpaf
open Aeio

let read fd buffer = assert false

let create_connection_handler ?config request_handler =
  fun client_addr fd ->
    let conn = Connection.create ?config (fun request request_body ->
      request_handler client_addr request request_body) in
    let rec reader_thread () =
      match Connection.next_read_operation conn with          
      | `Read buffer -> 
          let result = read fd buffer in
          Connection.report_read_result conn result;
          reader_thread ()
      | `Yield       -> Connection.yield_reader conn reader_thread
      | `Close _     -> Unix.(shutdown fd SHUTDOWN_RECEIVE)
    in
    let rec writer_thread () =
      match Connection.next_write_operation conn with
      | `Write iovecs -> assert false         
      | `Yield        -> Connection.yield_writer conn writer_thread
      | `Close _      -> Unix.(shutdown fd SHUTDOWN_SEND)
    in
    let t1 = async reader_thread () in      
    let t2 = async writer_thread () in
    ignore (await t1);
    ignore (await t2)
