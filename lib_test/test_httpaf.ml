open Result

let debug msg =
  if false then Printf.eprintf "%s\n%!" msg

type handler   = Connection.handler
type body_part = [ `Fixed of string | `Chunk of string ]

type input_stream  = [ `Request  of Request.t  | body_part ] list
type output_stream = [ `Response of Response.t | body_part ] list

let request_to_string r =
  let f = Faraday.create 0x1000 in
  Serialize.write_request f r;
  Faraday.serialize_to_string f

let response_to_string r =
  let f = Faraday.create 0x1000 in
  Serialize.write_response f r;
  Faraday.serialize_to_string f

let body_part_to_rev_strings = function
  | `Fixed x -> [x]
  | `Chunk x ->
    let len = String.length x in
    [x; Printf.sprintf "%x\r\n" len]

let input_stream_to_strings is =
  let rec loop is acc =
    match is with
    | []                      -> List.rev acc
    | `Request  r      :: is' -> loop is' (request_to_string r :: acc)
    | #body_part as bp :: is' -> loop is' (body_part_to_rev_strings bp @ acc)
  in
  loop is []

let output_stream_to_strings is =
  let rec loop is acc =
    match is with
    | []                       -> List.rev acc
    | `Response r       :: is' -> loop is' (response_to_string r :: acc)
    | #body_part  as bp :: is' -> loop is' (body_part_to_rev_strings bp @ acc)
  in
  loop is []

let iovec_to_string { IOVec.buffer; off; len } =
  match buffer with
  | `String x    -> String.sub x off len
  | `Bytes  x    -> Bytes.(to_string (sub x off len))
  | `Bigstring x -> Bigstring.copy ~off ~len x

let test ~msg ~input ~output ~handler =
  let input  = input_stream_to_strings input in
  let output = output_stream_to_strings output in
  let conn   = Connection.create handler in
  let iwait, owait = ref false, ref false in
  let rec loop conn input =
    if !iwait && !owait then
      assert false (* deadlock, at lest for test handlers. *);
    match Connection.state conn with
    | `Running ->
      debug "state: running";
      let input'  = iloop conn input in
      let output  = oloop conn in
      output @ loop conn input'
    | `Closed_input ->
      debug "state: closed_input";
      let output = oloop conn in
      output
    | `Closed ->
      debug "state: closed";
      []
  and iloop conn input =
    if !iwait
    then begin debug " iloop: wait"; input end
    else
      match Connection.next_read_operation conn, input with
      | `Read buffer, s::input' ->
        debug " iloop: read";
        let len = min (Bigstring.length buffer) (String.length s) in
        Bigstring.blit_from_string s 0 buffer 0 len;
        Connection.report_read_result conn (`Ok len);
        if len = String.length s
        then input'
        else String.(sub s len (length s - len)) :: input'
      | `Read _, [] ->
        debug " iloop: eof";
        Connection.report_read_result conn `Eof;
        []
      | _          , [] ->
        debug " iloop: eof";
        Connection.shutdown_reader conn; []
      | `Close (Error _)   , _     ->
        debug " iloop: close(error)";
        Connection.shutdown_reader conn; []
      | `Close (Ok _)   , _     ->
        debug " iloop: close(ok)";
        Connection.shutdown_reader conn; []
      | `Yield , _  ->
        debug " iloop: yield";
        iwait := true;
        Connection.yield_reader conn (fun () -> debug " iloop: continue"; iwait := false);
        input
  and oloop conn =
    if !owait
    then (begin debug " oloop: wait"; [] end)
    else
      match Connection.next_write_operation conn with
      | `Close _  ->
        debug " oloop: closed";
        Connection.shutdown conn;
        []
      | `Yield ->
        debug " oloop: yield";
        owait := true;
        Connection.yield_writer conn (fun () -> debug " oloop: continue"; owait := false);
        []
      | `Write iovecs ->
        debug " oloop: write";
        let output = List.map iovec_to_string iovecs in
        Connection.report_write_result conn (`Ok (IOVec.lengthv iovecs));
        output
  in
  debug ("=== " ^ msg);
  let test_output = loop conn input in
  assert (output = test_output)

let () =
  let handler body _ request_body start_response =
    debug " > handler called";
    Body.close request_body;
    let response_body = start_response (Response.create `OK) in
    Body.write_string response_body body;
    Body.close response_body
  in
  test ~msg:"Single OK" ~handler:(handler "")
    ~input:[`Request (Request.create `GET "/")]
    ~output:[`Response (Response.create `OK)];
  test ~msg:"Multiple OK" ~handler:(handler "")
    ~input:[`Request (Request.create `GET "/"); `Request (Request.create `GET "/")]
    ~output:[`Response (Response.create `OK); `Response (Response.create `OK)];
  test ~msg:"Conn close" ~handler:(handler "")
    ~input:[ `Request (Request.create ~headers:Headers.(of_list ["connection", "close"]) `GET "/")
           ; `Request (Request.create `GET "/")]
    ~output:[`Response (Response.create `OK)];
  test ~msg:"Single OK w/body" ~handler:(handler "Hello, world!")
    ~input:[ `Request (Request.create ~headers:Headers.(of_list ["connection", "close"]) `GET "/")]
    ~output:[`Response (Response.create `OK); `Fixed "Hello, world!" ];
  let echo _ request_body start_response =
    debug " > handler called";
    let response_body = start_response (Response.create ~headers:Headers.(of_list ["connection", "close"]) `OK) in
    let readv iovecs =
      let len = IOVec.lengthv iovecs in
      List.iter (fun { IOVec.buffer; off; len } ->
        begin match buffer with
        | `Bigstring bs -> Body.schedule_string response_body (Bigstring.copy ~off ~len bs)
        | `String s     -> Body.schedule_string response_body ~off ~len s
        | `Bytes bs     -> Body.schedule_string response_body ~off ~len (Bytes.to_string bs)
        end)
      iovecs;
      ((), len)
    in
    let rec result = function
      | `Eof  -> Body.close response_body
      | `Ok _ -> Body.schedule_read request_body ~readv ~result
    in
    Body.schedule_read request_body ~readv ~result
  in
  test ~msg:"POST" ~handler:echo
    ~input:[`Request (Request.create `GET "/" ~headers:Headers.(of_list ["transfer-encoding", "chunked"])); `Chunk "This is a test"]
    ~output:[`Response (Response.create `OK ~headers:Headers.(of_list ["connection", "close"])); `Fixed "This is a test"]
