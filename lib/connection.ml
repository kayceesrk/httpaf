(*----------------------------------------------------------------------------
    Copyright (c) 2016 Inhabited Type LLC.

    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in the
       documentation and/or other materials provided with the distribution.

    3. Neither the name of the author nor the names of his contributors
       may be used to endorse or promote products derived from this software
       without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE CONTRIBUTORS ``AS IS'' AND ANY EXPRESS
    OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED.  IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE FOR
    ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
    DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
    OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
    HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
    STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
  ----------------------------------------------------------------------------*)


open Result

module Reader = struct
  module AU = Angstrom.Unbuffered

  type error = [
    | Status.t
    | `Parse of string list * string ]

  type operations = [
    | `Close of (unit, error) result
    | `Read  of Bigstring.t * ([`Ok of int | `Eof] -> (unit, [`Invalid_read_length]) result)
  ]

  type t =
    { handler             : Request.t -> Body.R.t -> unit
      (* The application request handler. *)
    ; buffer              : Bigstring.t
      (* The buffer that the parser reads from. Managed by the control module
       * for the reader. *)
    ; mutable off         : int
      (* The start of the readable region of {buffer}. *)
    ; mutable len         : int
      (* The length of the readable region of {buffer}. *)
    ; mutable parse_state : (unit, error) result AU.state
      (* The state of the parse for the current request *)
    ; mutable closed      : bool
      (* Whether the input source has left the building, indicating that no
       * further input will be received. *)
    }

  let parser handler =
    let open Parse in
    let ok = return (Ok ()) in
    let p =
      request >>= fun request ->
      match Request.body_length request with
      | `Error err -> return (Error err)
      | `Fixed 0L  ->
        handler request Body.R.empty;
        ok
      | `Fixed _ | `Chunked | `Close_delimited as encoding ->
        let request_body, writer = Body.create ~buffer_size:0 () in
        handler request request_body;
        body ~encoding writer *> ok
    in
    Angstrom.z p

  let create ?(buffer_size=0x1000) handler =
    let buffer = Bigstring.create buffer_size in
    { handler
    ; buffer
    ; off         = 0
    ; len         = 0
    ; parse_state = AU.parse Angstrom.(parser handler)
    ; closed      = false
    }

  let invariant t =
    assert
      (match t.parse_state with
      | AU.Done(_, committed) | AU.Partial { AU.committed } -> committed = 0
      | AU.Fail _ -> true);
    assert (t.len <= Bigstring.length t.buffer);
    assert (t.off <  Bigstring.length t.buffer);
    assert (t.off >= 0);
  ;;

  let close t =
    t.closed <- true

  let is_closed t =
    t.closed

  let commit t n =
    let { off; len } = t in
    if n = len then begin
      t.off <- 0;
      t.len <- 0
    end else begin
      t.off <- t.off + n;
      t.len <- t.len - n
    end

  let buffer_for_reading { buffer; off; len } =
    `Bigstring (Bigstring.sub ~off ~len buffer)

  let update_parse_state t more =
    match t.parse_state with
    | AU.Fail _                 -> failwith "parse failed"
    | AU.Done(Error _, _)       -> failwith "NYI"
    | AU.Done(Ok (), committed) ->
      commit t committed;
      if more = AU.Incomplete
      then t.parse_state <- AU.parse (parser t.handler);
    | AU.Partial { AU.continue; committed } ->
      commit t committed;
      t.parse_state <- continue (buffer_for_reading t) more

  let report_result t result =
    match result with
    | `Ok 0   -> ()
    | `Ok len ->
      let len = t.len + len in
      if len <= Bigstring.length t.buffer then begin
        t.len <- len;
        update_parse_state t AU.Incomplete
      end else
        failwith "Reader.report_result size of read exceeds size of buffer"
    | `Eof ->
      close t

  let rec next t =
    match t.parse_state with
    | AU.Done(result, n) ->
      begin match result with
      | Ok () as result ->
        if t.closed then `Close result
        else begin update_parse_state t AU.Incomplete; next t end
      | Error err as result -> `Close result
      end
    | AU.Fail(marks , message)  ->
      Printf.printf "AU.Fail: %s\n%!" message;
      `Close (Error (`Parse(marks, message)))
    | AU.Partial { AU.committed } ->
      assert (committed = 0);
      if t.closed then begin update_parse_state t AU.Complete; next t end
      else
        let { buffer; off; len } = t in
        if len = Bigstring.length buffer
        then `Close(Error (`Parse([], "parser stall: input too large")))
        else `Read(Bigstring.sub ~off:(off + len) buffer)
end

module Writer = struct
  module F = Faraday

  type t =
    { buffer                : Bigstring.t
      (* The buffer that the encoder uses for buffered writes. Managed by the
       * control module for the encoder. *)
    ; encoder               : F.t
      (* The encoder that handles encoding for writes. Uses the [buffer]
       * referenced above internally. *)
    ; mutable closed        : bool
      (* Whether the output source has left the building, indicating that no
       * further output should be generated. *)
    ; mutable drained_bytes : int
      (* The number of bytes that were not written due to the output stream
       * being closed before all buffered output could be written. Useful for
       * detecting error cases. *)
    }

  let create ?(buffer_size=0x100) () =
    let buffer = Bigstring.create buffer_size in
    let encoder = F.of_bigstring buffer in
    { buffer
    ; encoder
    ; closed        = false
    ; drained_bytes = 0
    }

  let invariant t =
    let (=>) a b = b || (not a) in
    let (<=>) a b = (a => b) && (b => a) in
    let writev, close, yield =
      match F.operation t.encoder with
      | `Writev _ -> true, false, false
      | `Close    -> false, true, false
      | `Yield    -> F.yield t.encoder; false, false, true
    in
    assert (t.closed <=> F.is_closed t.encoder);
    assert (F.is_closed t.encoder <=> close);
    assert (t.drained_bytes > 0 => t.closed);
  ;;

  let write_response t response =
    Serialize.write_response t.encoder response

  let schedule_fixed t iovecs =
    (* XXX(seliopou): lots of closure allocation happening here, starting with
     * the function passed to [List.iter] and then once for each [IOVec.t].
     * Reconsider the type of the optional [free] argument in the faraday
     * library. If it took the buffer together with an off and len, then that
     * would eliminte the need for the closure allocations in the loop. *)
    let s2b = Bytes.unsafe_of_string in
    List.iter (fun { IOVec.buffer; off; len } ->
      match buffer with
      | `String str   -> F.schedule_bytes     t.encoder ~off ~len (s2b str)
      | `Bytes bytes  -> F.schedule_bytes     t.encoder ~off ~len bytes
      | `Bigstring bs -> F.schedule_bigstring t.encoder ~off ~len bs)
    iovecs

  let schedule_chunk t iovecs =
    let len = Int64.of_int (IOVec.lengthv iovecs) in
    F.write_string t.encoder (Printf.sprintf "%Lx\r\n" len);
    schedule_fixed t iovecs

  let flush t f =
    F.flush t.encoder f

  let close t =
    t.closed <- true;
    F.close t.encoder;
    let drained = F.drain t.encoder in
    t.drained_bytes <- t.drained_bytes + drained

  let drained_bytes t =
    t.drained_bytes

  let report_result t result =
    match result with
    | `Closed -> close t
    | `Ok len -> F.shift t.encoder len

  let next t =
    match F.operation t.encoder with
    | `Close -> `Close (drained_bytes t)
    | `Yield -> `Yield
    | `Writev iovecs ->
      assert (not (t.closed));
      `Write ((iovecs:IOVec.buffer IOVec.t list))
end

module Rd = struct
  type handlers =
    { mutable start_response : (unit -> unit) list
    ; mutable close_response : (unit -> unit) list
    ; mutable response_chunk : (unit -> unit) list
    }

  type response_state =
    | Waiting of handlers
    | Started of Response.t * Body.W.t

  type t =
    { request                 : Request.t
    ; request_body            : Body.R.t
    ; buffered_response_bytes : int ref
    ; mutable response_state  : response_state
    }

  let empty_handlers () =
    { start_response = []
    ; close_response = []
    ; response_chunk = []
    }

  let create request request_body =
    { request
    ; request_body
    ; buffered_response_bytes = ref 0
    ; response_state          = Waiting (empty_handlers ())
    }

  let finish_request t =
    Body.close t.request_body

  let start_response t response response_body =
    match t.response_state with
    | Started _        -> assert false
    | Waiting handlers ->
      t.response_state <- Started(response, response_body);
      List.iter (fun f -> Body.on_close response_body f) handlers.close_response;
      List.iter (fun f -> f ()) handlers.start_response;
      List.iter (fun f -> Body.on_more_output_available response_body f)
        handlers.response_chunk
  ;;

  let persistent_connection t =
    Request.persistent_connection t.request
    && match t.response_state with
    | Waiting _            -> true
    | Started(response, _) -> Response.persistent_connection response

  let requires_input { request_body } =
    not (Body.is_closed request_body)

  let requires_output { response_state } =
    match response_state with
    | Waiting _                 -> true
    | Started(_, response_body) ->
      Body.(has_pending_output response_body || not (is_closed response_body))

  let is_complete t =
    not (requires_input t || requires_output t)

  let flush_request_body t =
    if Body.has_pending_output t.request_body then
      Body.execute_read t.request_body

  let flush_response_body t writer =
    match t.response_state with
    | Waiting _                        -> ()
    | Started(response, response_body) ->
      let faraday = response_body.Body.faraday in
      begin match Faraday.operation faraday with
      | `Yield | `Close -> ()
      | `Writev iovecs ->
        let buffered = t.buffered_response_bytes in
        let iovecs   = IOVec.shiftv iovecs !buffered in
        let lengthv  = IOVec.lengthv iovecs in
        buffered := !buffered + lengthv;
        let request_method = t.request.Request.meth in
        begin match Response.body_length ~request_method response with
        | `Fixed _ | `Close_delimited -> Writer.schedule_fixed writer iovecs
        | `Chunked -> Writer.schedule_chunk writer iovecs
        | `Error _ -> assert false
        end;
        Writer.flush writer (fun () ->
          Faraday.shift faraday lengthv;
          buffered := !buffered - lengthv)
      end

  let on_more_output_available t k =
    match t.response_state with
    | Waiting handlers          -> handlers.response_chunk <- k::handlers.response_chunk
    | Started(_, response_body) -> Body.on_more_output_available response_body k

  let on_response_available t k =
    match t.response_state with
    | Waiting handlers          -> handlers.start_response <- k::handlers.start_response
    | Started(_, response_body) -> k ()

  let on_response_complete t k =
    match t.response_state with
    | Waiting handlers          -> handlers.close_response <- k::handlers.close_response
    | Started(_, response_body) -> Body.on_close response_body k

  let invariant t =
    let (=>) a b = b || not a in
    assert (is_complete t => not (requires_input t));
    assert (is_complete t => not (requires_output t));
  ;;
end

module Config = struct
  type t =
    { read_buffer_size        : int
    ; write_buffer_size       : int
    }

  let default =
    { read_buffer_size        = 0x1000
    ; write_buffer_size       = 0x1000 }
end

type handler =
  Request.t -> Body.R.t -> (Response.t -> Body.W.t) -> unit

type t =
  { reader                  : Reader.t
  ; writer                  : Writer.t
  ; user_handler            : handler
  ; request_queue           : Rd.t Queue.t
  ; mutable current_request : Rd.t option
  ; mutable on_advance      : (unit -> unit) list
  ; mutable on_flush        : (unit -> unit) list
  }

let invariant t =
  let (=>) a b = b || (not a) in
  Reader.invariant t.reader;
  Writer.invariant t.writer;
  assert (t.current_request = None => Queue.is_empty t.request_queue);
  assert (t.writer.Writer.closed => t.reader.Reader.closed);
;;

let create ?(config=Config.default) user_handler =
  let
    { Config
    . read_buffer_size
    ; write_buffer_size
    } = config
  in
  let request_queue = Queue.create () in
  let handler request request_body =
    Queue.push (Rd.create request request_body) request_queue in
  { reader          = Reader.create ~buffer_size:read_buffer_size handler
  ; writer          = Writer.create ~buffer_size:write_buffer_size ()
  ; user_handler
  ; current_request = None
  ; request_queue
  ; on_advance      = []
  ; on_flush        = []
  }

let state t =
  match t.reader.Reader.closed, t.writer.Writer.closed with
  | false, false -> `Running
  | true , true  -> `Closed
  | true , false -> `Closed_input
  | false, true  -> assert false

let on_flush t k =
  if state t = `Closed
  then failwith "on flush on closed conn"
  else t.on_flush <- k::t.on_advance

let on_advance t k =
  if state t = `Closed
  then failwith "on_advanced on closed conn"
  else t.on_advance <- k::t.on_advance

let fire_advance t =
  let callbacks = t.on_advance in
  t.on_advance <- [];
  List.iter (fun f -> f ()) callbacks

let fire_flush t =
  let callbacks = t.on_flush in
  t.on_flush <- [];
  List.iter (fun f -> f ()) callbacks

let shutdown_reader t =
  Reader.close t.reader

let shutdown_writer t =
  Writer.close t.writer

let shutdown t =
  shutdown_reader t;
  shutdown_writer t;
  fire_advance t

let drain_request_queue t =
  Queue.iter (fun rd -> Rd.finish_request rd) t.request_queue;
  Queue.clear t.request_queue;
  fire_advance t

let advance_request_queue t =
  let { Rd.request; request_body } as rd = Queue.take t.request_queue in
  t.current_request <- Some rd;
  let request_method = request.Request.meth in
  t.user_handler request request_body (fun response ->
    match Response.body_length ~request_method response with
    | `Error err -> assert false (* XXX(seliopou): handle *)
    | _          ->
      Writer.write_response t.writer response;
      let _, response_body = Body.create () in (* XXX(seliopou): buffer size *)
      Rd.start_response rd response response_body;
      response_body);
  fire_advance t

let advance_request_queue_if_necessary t =
  match t.current_request with
  | None    ->
    if not (Queue.is_empty t.request_queue) then
      advance_request_queue t
    else if Reader.is_closed t.reader then begin
      shutdown t
    end
  | Some rd ->
    if Rd.persistent_connection rd then begin
      if Rd.is_complete rd then begin
        t.current_request <- None;
        fire_flush t
      end;
      if not (Queue.is_empty t.request_queue) then advance_request_queue t;
    end else begin
      drain_request_queue t;
      if Rd.is_complete rd then begin
        t.current_request <- None;
        shutdown t;
        fire_flush t;
      end else if not (Rd.requires_input rd) then
        shutdown_reader t
    end

let rec next_read_operation t =
  advance_request_queue_if_necessary t;
  match t.current_request with
  | None -> Reader.next t.reader
  | Some rd ->
    if Rd.requires_input rd then Reader.next t.reader
    else if Rd.persistent_connection rd
    then `Yield
    else begin
      shutdown_reader t;
      next_read_operation t
    end

let report_read_result t result =
  Reader.report_result t.reader result;
  match t.current_request with
  | None    -> ()
  | Some rd -> Rd.flush_request_body rd

let yield_reader t k =
  on_flush t k

let flush_response_body t =
  match t.current_request with
  | None    -> ()
  | Some rd -> Rd.flush_response_body rd t.writer

let rec next_write_operation t =
  advance_request_queue_if_necessary t;
  flush_response_body t;
  Writer.next t.writer

let report_write_result t result =
  Writer.report_result t.writer result

let yield_writer t k =
  match t.current_request with
  | None    -> 
      Printf.printf "yield_writer.on_advance\n%!";
      on_advance t k
  | Some rd ->
    if Rd.requires_output rd
    then Rd.on_more_output_available rd k
    else if Rd.persistent_connection rd
    then on_advance t k
    else begin shutdown t; k () end
