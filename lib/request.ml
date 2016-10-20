type t =
  { meth    : Method.t
  ; target  : string
  ; version : Version.t
  ; headers : Headers.t }

let create ?(version=Version.v1_1) ?(headers=Headers.empty) meth target =
  { meth; target; version; headers }

let body_length { headers } =
  (* XXX(seliopou): perform proper transfer-encoding parsing *)
  match Headers.get_multi headers "transfer-encoding" with
  | "chunked"::_                             -> `Chunked
  | _        ::es when List.mem "chunked" es -> `Error `Bad_request
  | [] | _                                   ->
    begin match
      (* XXX(seliopou): perform proper content-length parsing *)
      List.sort_uniq String.compare
        (Headers.get_multi headers "content-length")
    with
    | [len]   ->
      (try
        let len = Int64.of_string len in
        if Int64.compare len 0L < 0 then `Error `Bad_request else `Fixed len
      with _  -> `Error `Bad_request)
    | _::_::_ -> `Error `Bad_request
    | []      -> `Fixed 0L
    end

let persistent_connection ?proxy { version; headers } =
  Message.persistent_connection ?proxy version headers

let pp_hum fmt { meth; target; version; headers } =
  Format.fprintf fmt "((method \"%a\") (target %S) (version \"%a\") (headers %a))"
    Method.pp_hum meth target Version.pp_hum version Headers.pp_hum headers
