open Httpaf

exception Partial

let take_group iovecs =
  let to_group = function
    | { Faraday.buffer = `String buffer; off; len } ->
      `String [{ Faraday.buffer; off; len }]
    | { Faraday.buffer = `Bytes  buffer; off; len } ->
      `String [{ Faraday.buffer = Bytes.unsafe_to_string buffer; off; len }]
    | { Faraday.buffer = `Bigstring buffer; off; len } ->
      `Bigstring [{ Faraday.buffer; off; len }]
  in
  let rec loop group iovecs =
    match iovecs with
    | [] -> (group, [])
    | iovec::iovecs ->
      begin match to_group iovec, group with
      | `String item   , `String group ->
        loop (`String (item @ group)) iovecs
      | `Bigstring item, `Bigstring group ->
        loop (`Bigstring (item @ group)) iovecs
      | item, _ -> group, (iovec::iovecs)
      end
  in
  match iovecs with
  | [] -> None
  | iovec::iovecs ->
    let group, rest = loop (to_group iovec) iovecs in
    Some(group, rest)

let get_iovec loc ?pos ?len true_len buf =
  let pos =
    match pos with
    | None -> 0
    | Some pos ->
        if pos < 0 then invalid_arg (loc ^ ": pos < 0");
        pos
  in
  let len =
    match len with
    | None -> true_len
    | Some len ->
        if len < 0 then invalid_arg (loc ^ ": len < 0");
        len
  in
  if pos + len > true_len then invalid_arg (loc ^ ": pos + len > length buf");
  Httpaf.IOVec.({
    buffer = buf;
    off = pos;
    len = len;
  })

let unix_iovec_of_string ?pos ?len str = 
  let str_len = String.length str in
  get_iovec "IOVec.of_string" ?pos ?len str_len str

let unix_iovec_of_bigstring ?pos ?len bstr = 
  let bstr_len = Bigarray.Array1.dim bstr in
  get_iovec "IOVec.of_bigstring" ?pos ?len bstr_len bstr

let create_connection_handler ?config request_handler =
  fun fd client_addr ->
    let conn = Connection.create ?config (fun request request_body ->
      request_handler client_addr request request_body) in
    let ctxt = Aeio.new_context () in
    let rec reader_thread () =
      match Connection.next_read_operation conn with          
      | `Read buffer -> 
          let read_len = 
            try Aeio.Bigstring.read_all fd buffer 
            with e -> 
              Printf.printf "reader_thread raised %s\n%!" @@ Printexc.to_string e;
              Unix.(shutdown fd SHUTDOWN_RECEIVE); 
              Aeio.cancel ctxt;
              raise e
          in
          if read_len = 0 then begin
            Connection.report_read_result conn `Eof;
            Aeio.cancel ctxt
          end else begin
            Connection.report_read_result conn (`Ok read_len);
            reader_thread ()
          end
      | `Yield       -> 
          let iv = Aeio.IVar.create () in
          Connection.yield_reader conn (Aeio.IVar.fill iv);
          Aeio.IVar.read iv;
          reader_thread ()
      | `Close status -> Unix.(shutdown fd SHUTDOWN_RECEIVE)
    in
    let rec writer_thread () =
      let success = Connection.report_write_result conn in
      match Connection.next_write_operation conn with
      | `Write iovecs -> 
          begin match take_group iovecs with
          | None -> success (`Ok 0)
          | Some (`String group, _) -> 
            let iovecs = Array.of_list (List.rev_map (fun iovec ->
              let { Faraday.buffer; off = pos; len } = iovec in
              unix_iovec_of_string ~pos ~len buffer) group)
            in 
            let written = ref 0 in
            begin try
              Array.iter (fun {Faraday.buffer; off; len} -> 
                let w = Aeio.write fd buffer off len in
                written := !written + w;
                if w < len then raise Partial) iovecs;
              success (`Ok !written)
            with
            | Partial -> success (`Ok !written)
            | e ->
                Printf.printf "writer_thread raised %s\n%!" @@ Printexc.to_string e;
                Unix.(shutdown fd SHUTDOWN_SEND);
                Aeio.cancel ctxt
            end
          | Some (`Bigstring group, _) ->
            let iovecs = Array.of_list (List.rev_map (fun iovec ->
              let { Faraday.buffer; off = pos; len } = iovec in
              unix_iovec_of_bigstring ~pos ~len buffer) group)
            in 
            let written = ref 0 in
            begin try
              Array.iter (fun {Faraday.buffer; off; len} -> 
                let w = Aeio.Bigstring.write fd buffer off len in
                written := !written + w;
                if w < len then raise Partial) iovecs;
              success (`Ok !written)
            with
            | Partial -> success (`Ok !written)
            | e ->
                Printf.printf "writer_thread raised %s\n%!" @@ Printexc.to_string e;
                Unix.(shutdown fd SHUTDOWN_SEND);
                raise e
            end
          end;
          writer_thread ()
      | `Yield        -> 
          let iv = Aeio.IVar.create () in
          Connection.yield_writer conn (Aeio.IVar.fill iv);
          Aeio.IVar.read iv;
          writer_thread ()
      | `Close _      -> 
          Unix.(shutdown fd SHUTDOWN_SEND)
    in
    ignore @@ Aeio.async ~ctxt reader_thread ();
    Aeio.async ~ctxt writer_thread ()
