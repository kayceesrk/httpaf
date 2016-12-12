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

  let update_parse_state t parse_state =
    match parse_state with
    | AU.Done(_, 0) | AU.Fail _ -> t.parse_state <- parse_state
    | AU.Done(result, committed) ->
      commit t committed;
      t.parse_state <- AU.Done(result, 0);
    | AU.Partial { AU.committed; continue } ->
      commit t committed;
      t.parse_state <- AU.(Partial { committed = 0; continue })

  let buffer_for_reading { buffer; off; len } =
    `Bigstring (Bigstring.sub ~off ~len buffer)

  let rec next t =
    match t.parse_state with
    | AU.Done(result, n) ->
      assert (n = 0);
      begin match result with
      | Ok () as result ->
        if t.closed then
          `Close result
        else begin
          update_parse_state t (AU.parse (parser t.handler));
          next t
        end
      | Error err as result -> `Close result
      end
    | AU.Fail(marks , message)  ->
      `Close (Error (`Parse(marks, message)))
    | AU.Partial p              ->
      let { AU.continue } = p in
      if t.closed then begin
        update_parse_state t (continue (buffer_for_reading t) AU.Complete);
        next t
      end else
        let { buffer; off; len } = t in
        if len = Bigstring.length buffer
        then `Close (Error (`Parse([], "parser stall: input too large")))
        else
          `Read(Bigstring.sub ~off:(off + len) buffer, function
            | `Ok 0   -> Ok ()
            | `Ok len ->
              let len' = t.len + len in
              if len' <= Bigstring.length t.buffer then begin
                t.len <- len';
                update_parse_state t (continue (buffer_for_reading t) AU.Incomplete);
                Ok ()
              end else
                Error `Invalid_read_length
            | `Eof    -> close t; Ok ())
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

  let next t =
    match F.operation t.encoder with
    | `Close -> `Close (drained_bytes t)
    | `Yield -> `Yield
    | `Writev iovecs ->
      assert (not (t.closed));
      `Write ((iovecs:IOVec.buffer IOVec.t list), function
        | `Closed -> close t
        | `Ok len -> F.shift t.encoder len)
end
