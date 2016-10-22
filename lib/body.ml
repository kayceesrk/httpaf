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

type bigstring = Bigstring.t
type buffer = IOVec.buffer
type 'a iovec = 'a IOVec.t

type reader =
  | R : (buffer iovec list -> 'a * int) * ([`Eof | `Ok of 'a] -> unit) -> reader
  | N : reader

type 'mode t =
  { faraday           : Faraday.t
  ; mutable reader    : reader
  ; mutable on_close  : (unit -> unit) list
  ; mutable on_write  : (unit -> unit) list
  }

let create ?(buffer_size=0x1000) () =
  let faraday = Faraday.create buffer_size in
  let t =
    { faraday
    ; reader    = N
    ; on_close  = []
    ; on_write  = []
    }
  in
  t, t

let empty () = create ~buffer_size:0 ()

let of_string ?off ?len s =
  let t = Faraday.create 0 in
  Faraday.schedule_string t ?off ?len s;
  t

let of_strings ss =
  let t = Faraday.create 0 in
  List.iter (fun s ->
    Faraday.schedule_string t s)
  ss;
  t

let schedule_read t ~readv ~result =
  match t.reader with
  | N   -> t.reader <- R(readv, result)
  | R _ -> raise (Failure "Body.schedule_read: reader already scheduled")

let execute_read t =
  match t.reader with
  | N                -> ()
  | R(readv, result) ->
    begin match Faraday.operation t.faraday with
    | `Yield         -> ()
    | `Close         -> t.reader <- N; result `Eof
    | `Writev iovecs ->
      t.reader <- N;
      let a, n = readv iovecs in
      assert (n >= 0);
      result (`Ok a)
    end

let fire_on_write t =
  let callbacks = t.on_write in
  t.on_write <- [];
  List.iter (fun f -> f ()) callbacks

let fire_on_close t =
  let callbacks = t.on_close in
  t.on_close <- [];
  List.iter (fun f -> f ()) callbacks

let write_string t ?off ?len s =
  Faraday.write_string t.faraday ?off ?len s;
  fire_on_write t

let write_bigstring t ?off ?len (s:bigstring) =
  (* XXX(seliopou): there is a type annontation on bigstring because of bug
   * #1699 on the OASIS bug tracker. Once that's resolved, it should no longer
   * be necessary. *)
  Faraday.write_bigstring t.faraday ?off ?len s;
  fire_on_write t

let schedule_string t ?off ?len s =
  Faraday.schedule_string t.faraday ?off ?len s;
  fire_on_write t

let schedule_bigstring t ?off ?len (s:bigstring) =
  Faraday.schedule_bigstring t.faraday ?off ?len s;
  fire_on_write t

let flush t f =
  Faraday.flush t.faraday f

let close t =
  Faraday.close t.faraday;
  fire_on_write t;
  fire_on_close t

let is_closed t =
  Faraday.is_closed t.faraday

let has_pending_output t =
  Faraday.has_pending_output t.faraday

let empty_closed =
  let r, w as b = create ~buffer_size:0 () in
  close r;
  b

module R = struct
  type phantom
  type nonrec t = phantom t

  let empty = fst empty_closed
end

module W = struct
  type phantom
  type nonrec t = phantom t

  let empty = snd empty_closed
end

(* Callbacks *)

let on_close t k =
  if is_closed t
  then k ()
  else t.on_close <- k::t.on_close

let on_more_output_available t k =
  if is_closed t
  then failwith "body closed"
  else t.on_write <- k::t.on_write
