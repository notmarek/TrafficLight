from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import TYPE_CHECKING

from aiohttp import web

from .base import BaseOutput

if TYPE_CHECKING:
    from trafficlight.proto_utils.proto import Proto


class WebOutput(BaseOutput):
    def __init__(self) -> None:
        self._clients: set[web.WebSocketResponse] = set()
        self._records: list[dict] = []
        self._app: web.Application | None = None

    def setup_routes(self, app: web.Application) -> None:
        """Add web UI routes to the existing aiohttp app."""
        self._app = app
        app.add_routes([
            web.get("/", self._handle_index),
            web.get("/ws", self._handle_websocket),
        ])

    async def start(self) -> None:
        pass  # Routes are added via setup_routes() before the server runs

    async def _handle_index(self, _request: web.Request) -> web.Response:
        return web.Response(text=HTML_PAGE, content_type="text/html")

    async def _handle_websocket(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._clients.add(ws)

        # Send existing records for catch-up
        try:
            for record in self._records:
                await ws.send_json(record)
        except Exception:
            self._clients.discard(ws)
            return ws

        try:
            async for _msg in ws:
                pass  # Client only sends keepalive; we broadcast
        finally:
            self._clients.discard(ws)

        return ws

    async def add_record(
        self,
        rpc_id: int,
        rpc_status: int,
        protos: list[Proto],
        rpc_handle: int | None = None,
    ) -> None:
        record = _format_record(rpc_id, rpc_status, protos, rpc_handle)
        self._records.append(record)

        # Cap stored records
        if len(self._records) > 500:
            self._records = self._records[-500:]

        # Broadcast to all connected clients
        stale: set[web.WebSocketResponse] = set()
        for ws in self._clients:
            try:
                await ws.send_json(record)
            except Exception:
                stale.add(ws)
        self._clients -= stale


def _format_record(
    rpc_id: int,
    rpc_status: int,
    protos: list[Proto],
    rpc_handle: int | None,
) -> dict:
    time_str = datetime.now().strftime("%H:%M:%S")
    formatted_protos = [_format_proto(p) for p in protos]
    return {
        "type": "record",
        "time": time_str,
        "rpc_id": rpc_id,
        "rpc_status": rpc_status,
        "rpc_status_name": RPC_STATUS_NAMES.get(rpc_status, "Unknown"),
        "rpc_handle": rpc_handle,
        "protos": formatted_protos,
    }


def _format_proto(proto: Proto) -> dict:
    result: dict = {
        "method_value": proto.method_value,
        "method_name": proto.method_name or "Unknown Method",
        "request": _format_message(proto.request),
        "response": _format_message(proto.response),
    }
    if proto.proxy:
        result["proxy"] = _format_proto(proto.proxy)
    return result


def _is_map_entry(field) -> bool:
    """Check if a field descriptor is a protobuf map entry."""
    from google.protobuf import descriptor as _descriptor

    return (
        field.type == _descriptor.FieldDescriptor.TYPE_MESSAGE
        and field.message_type.has_options
        and field.message_type.GetOptions().map_entry
    )


def _format_message(message) -> dict:
    if message.name is None:
        return {
            "name": "Unknown Message",
            "type": message.type,
            "error": True,
            "blackbox": json.dumps(message.blackbox, indent=2, ensure_ascii=False)
            if message.blackbox
            else "No data",
        }

    result: dict = {
        "name": message.name,
        "type": message.type,
        "error": False,
        "fields": [],
    }

    if message.payload is not None:
        result["fields"] = _format_message_fields(message.payload)

    elif message.blackbox:
        result["error"] = True
        result["blackbox"] = json.dumps(message.blackbox, indent=2, ensure_ascii=False)

    return result


def _format_message_fields(msg, _depth: int = 0) -> list[dict]:
    """Walk a protobuf message's ListFields() output, returning a list of field dicts."""
    from google.protobuf import descriptor as _descriptor

    result: list[dict] = []
    fields = msg.ListFields()
    fields = sorted(
        fields,
        key=lambda f: 1 if f[0].cpp_type == _descriptor.FieldDescriptor.CPPTYPE_MESSAGE else 0,
    )
    for field, value in fields:
        if _is_map_entry(field):
            for key in sorted(value):
                entry_submsg = value.GetEntryClass()(key=key, value=value[key])
                result.append(_format_field(field, entry_submsg, _depth))
        elif field.label == _descriptor.FieldDescriptor.LABEL_REPEATED:
            idx = 0
            for element in value:
                result.append(_format_field(field, element, _depth, repeated_index=idx))
                idx += 1
        else:
            result.append(_format_field(field, value, _depth))

    return result


def _format_single_message(msg, _depth: int = 0, label: str = "") -> dict:
    """Format a single protobuf message as a nested field dict."""
    if not msg.ListFields():
        return {"name": label, "type": "message", "label": "single", "value": "{}", "empty": True}
    return {
        "name": label,
        "type": "message",
        "label": "single",
        "value": _format_message_fields(msg, _depth),
        "nested": True,
    }


def _format_field(field, value, _depth: int = 0, repeated_index: int | None = None) -> dict:
    """Serialize a protobuf field value into a JSON-friendly dict.

    When *repeated_index* is not None, the value is a single element of a
    repeated field — treat it as a scalar message even though the field
    descriptor still says LABEL_REPEATED.
    """
    from google.protobuf import descriptor as _descriptor

    is_repeated = field.label == _descriptor.FieldDescriptor.LABEL_REPEATED and repeated_index is None

    field_info: dict = {
        "name": field.name,
        "type": TYPES.get(field.type, f"type_{field.type}"),
        "label": "repeated" if is_repeated else "single",
    }
    if repeated_index is not None:
        field_info["name"] = f"{field.name}[{repeated_index}]"

    if field.cpp_type == _descriptor.FieldDescriptor.CPPTYPE_MESSAGE:
        # Map entry sub-message — must come before is_repeated since map
        # fields are technically repeated too
        if _is_map_entry(field):
            entry_fields = sorted(
                value.ListFields(),
                key=lambda f: 1 if f[0].cpp_type == _descriptor.FieldDescriptor.CPPTYPE_MESSAGE else 0,
            )
            field_info["value"] = [_format_field(sf, sv, _depth + 1) for sf, sv in entry_fields]
            field_info["nested"] = True
        # A repeated composite container that hasn't been expanded yet
        elif is_repeated:
            if _depth > 3:
                field_info["value"] = "[...]"
                field_info["truncated"] = True
            else:
                # value is a RepeatedCompositeContainer — iterate its elements
                elements: list[dict] = []
                for idx, elem in enumerate(value):
                    elements.append(_format_single_message(elem, _depth + 1, f"{field.name}[{idx}]"))
                field_info["value"] = elements
                field_info["nested"] = True
        elif _depth > 3:
            field_info["value"] = "{...}"
            field_info["truncated"] = True
        elif not value.ListFields():
            field_info["value"] = "{}"
            field_info["empty"] = True
        else:
            field_info["value"] = _format_message_fields(value, _depth + 1)
            field_info["nested"] = True
    elif field.cpp_type == _descriptor.FieldDescriptor.CPPTYPE_ENUM:
        enum_value = field.enum_type.values_by_number.get(value, None)
        field_info["value"] = f"{field.enum_type.name}.{enum_value.name}:{value}" if enum_value else str(value)
        field_info["css_class"] = "enum"
    elif field.cpp_type == _descriptor.FieldDescriptor.CPPTYPE_STRING:
        field_info["value"] = json.dumps(str(value), ensure_ascii=False)
        field_info["css_class"] = "string"
    elif field.cpp_type == _descriptor.FieldDescriptor.CPPTYPE_BOOL:
        field_info["value"] = "true" if value else "false"
        field_info["css_class"] = "bool"
    elif field.cpp_type in (
        _descriptor.FieldDescriptor.CPPTYPE_FLOAT,
        _descriptor.FieldDescriptor.CPPTYPE_DOUBLE,
        _descriptor.FieldDescriptor.CPPTYPE_INT32,
        _descriptor.FieldDescriptor.CPPTYPE_INT64,
        _descriptor.FieldDescriptor.CPPTYPE_UINT32,
        _descriptor.FieldDescriptor.CPPTYPE_UINT64,
    ):
        field_info["value"] = str(value)
        field_info["css_class"] = "number"
    else:
        field_info["value"] = str(value)
        field_info["css_class"] = "other"

    return field_info


# RPC status name mapping
RPC_STATUS_NAMES = {
    0: "Undefined",
    1: "Success",
    3: "BadResponse",
    4: "ActionError",
    5: "DispatchError",
    6: "ServerError",
    7: "AssignmentError",
    8: "ProtocolError",
    9: "AuthenticationError",
    10: "CancelledRequest",
    11: "UnknownError",
    12: "NoRetriesError",
    13: "UnauthorizedError",
    14: "ParsingError",
    15: "AccessDenied",
    16: "AccessSuspended",
    17: "DeviceIncompatible",
    18: "AccessRateLimited",
    19: "GooglePlayNotReady",
    20: "LoginErrorBail",
}

# Protobuf field type mapping
TYPES = {
    1: "double",
    2: "float",
    3: "int64",
    4: "uint64",
    5: "int32",
    6: "fixed64",
    7: "fixed32",
    8: "bool",
    9: "string",
    10: "group",
    11: "message",
    12: "bytes",
    13: "uint32",
    14: "enum",
    15: "sfixed32",
    16: "sfixed64",
    17: "sint32",
    18: "sint64",
}

# --- Embedded single-page web UI ---

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Traffic Light</title>
<style>
/* === Reset & Base === */
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;overflow:hidden}
body{
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;
  font-size:13px;background:#111;color:#ccc;display:flex;flex-direction:column;height:100vh}

/* === Header === */
#header{
  display:flex;align-items:center;gap:16px;padding:0 20px;height:48px;
  background:#1a1a1a;border-bottom:1px solid #333;flex-shrink:0;user-select:none}
#header .logo{display:flex;align-items:center;gap:8px;font-weight:600;font-size:15px;color:#e0e0e0}
#header .logo span{font-size:18px}
#header .badge{padding:3px 10px;border-radius:99px;font-size:11px;font-weight:500}
#header .badge-count{background:#2a2a2a;color:#999}
#header .badge-ok{background:#2a2a2a;color:#ccc}
#header .badge-err{background:#2a2a2a;color:#888}
#header .spacer{flex:1}
#header .summary{font-size:12px;color:#777}

/* === Toolbar === */
#toolbar{
  display:flex;align-items:center;gap:10px;padding:0 20px;height:42px;
  background:#1a1a1a;border-bottom:1px solid #333;flex-shrink:0}
#toolbar .filter-wrap{flex:1;position:relative;display:flex;align-items:center}
#toolbar .filter-wrap input{
  width:100%;background:#111;border:1px solid #333;color:#e0e0e0;
  padding:6px 12px 6px 32px;border-radius:6px;font-size:13px;outline:none;
  transition:border-color .15s}
#toolbar .filter-wrap input:focus{border-color:#666}
#toolbar .filter-wrap input::placeholder{color:#666}
#toolbar .filter-wrap .search-icon{position:absolute;left:10px;color:#666;font-size:13px;pointer-events:none}
#toolbar .filter-wrap .filter-count{position:absolute;right:10px;font-size:11px;color:#777;pointer-events:none}
#toolbar button{
  display:flex;align-items:center;gap:6px;padding:6px 14px;border-radius:6px;
  border:1px solid #333;background:transparent;color:#999;cursor:pointer;
  font-size:12px;font-weight:500;transition:all .15s;white-space:nowrap;font-family:inherit}
#toolbar button:hover{background:#2a2a2a;color:#e0e0e0}
#toolbar button.is-active{background:#333;border-color:#666;color:#e0e0e0}
#toolbar button.danger:hover{background:#2a2a2a;border-color:#888;color:#e0e0e0}
#toolbar .sep{width:1px;height:20px;background:#333;margin:0 2px}

/* === Body === */
#body{display:flex;flex:1;overflow:hidden}

/* === Sidebar (request list) === */
#sidebar{
  width:380px;min-width:280px;max-width:55%;display:flex;flex-direction:column;
  background:#111;border-right:1px solid #333;overflow:hidden}
#sidebar .list-header{
  display:flex;align-items:center;padding:0 16px;height:34px;
  font-size:11px;font-weight:600;color:#777;text-transform:uppercase;
  letter-spacing:.05em;border-bottom:1px solid #222;flex-shrink:0}
#sidebar .list-header .col-time{margin-left:4px;width:72px}
#sidebar .list-header .col-meta{flex:1}
#sidebar .list-body{flex:1;overflow-y:auto;overflow-x:hidden}
#sidebar .list-body::-webkit-scrollbar{width:5px}
#sidebar .list-body::-webkit-scrollbar-track{background:transparent}
#sidebar .list-body::-webkit-scrollbar-thumb{background:#333;border-radius:3px}

.request{
  display:flex;align-items:flex-start;gap:0;padding:6px 12px;cursor:pointer;
  border-bottom:1px solid #1a1a1a;transition:background .1s;user-select:none;
  border-left:3px solid transparent;min-height:40px}
.request:hover{background:#222}
.request.selected{background:#252525;border-left-color:#888}
.request.hidden{display:none}
.request .col-time{
  width:72px;flex-shrink:0;font-family:'SF Mono','Fira Code','Cascadia Code',monospace;
  font-size:11px;color:#777;padding-top:2px}
.request .col-meta{flex:1;min-width:0}
.request .col-meta .rpc-line{display:flex;align-items:center;gap:8px}
.request .col-meta .rpc-line .rpc-id{font-weight:600;font-size:12px;color:#e0e0e0}
.request .col-meta .rpc-line .rpc-status{
  font-size:10px;font-weight:600;padding:1px 7px;border-radius:99px;
  background:#2a2a2a;color:#999}
.rpc-status.s-ok,.request .col-meta .rpc-line .rpc-status.s-ok{background:#2a2a2a;color:#ddd;font-weight:700}
.rpc-status.s-none,.request .col-meta .rpc-line .rpc-status.s-none{background:#222;color:#777}
.rpc-status.s-err,.request .col-meta .rpc-line .rpc-status.s-err{background:#2a2a2a;color:#aaa;border:1px solid #555}
.request .col-meta .rpc-line .rpc-handle{font-size:11px;color:#666}
.request .col-meta .methods{font-size:11px;color:#aaa;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.request .col-meta .methods .sep{color:#444;margin:0 4px}
.request .chevron{flex-shrink:0;color:#444;font-size:10px;padding-top:2px;transition:transform .15s}
.request.expanded .chevron{transform:rotate(90deg)}
.request .req-detail{display:none;flex-basis:100%}
.request.expanded .req-detail{display:block;margin-top:6px;padding-top:6px;border-top:1px solid #333}

/* Sidebar resize handle */
#resize-handle{
  width:4px;cursor:col-resize;background:transparent;flex-shrink:0;
  transition:background .15s;position:relative;z-index:10}
#resize-handle:hover,#resize-handle.dragging{background:#888}

/* === Main content === */
#main{
  flex:1;display:flex;flex-direction:column;overflow:hidden;background:#0d0d0d}
#main .empty-state{
  display:flex;flex-direction:column;align-items:center;justify-content:center;
  height:100%;color:#555;gap:12px}
#main .empty-state .icon{font-size:48px;opacity:.4}
#main .empty-state p{font-size:14px}

/* Proto tabs */
.proto-tabs{
  display:flex;gap:0;padding:0 16px;border-bottom:1px solid #333;
  background:#111;flex-shrink:0;overflow-x:auto}
.proto-tabs::-webkit-scrollbar{height:3px}
.proto-tabs button{
  padding:10px 16px;border:none;background:transparent;color:#777;
  cursor:pointer;font-size:12px;font-weight:500;border-bottom:2px solid transparent;
  transition:all .15s;white-space:nowrap;font-family:inherit}
.proto-tabs button:hover{color:#aaa}
.proto-tabs button.active{color:#e0e0e0;border-bottom-color:#888}

/* Inspect content */
#inspect-content{flex:1;overflow-y:auto;padding:16px 24px}
#inspect-content::-webkit-scrollbar{width:5px}
#inspect-content::-webkit-scrollbar-track{background:transparent}
#inspect-content::-webkit-scrollbar-thumb{background:#333;border-radius:3px}

/* Message tabs */
.msg-tabs{display:flex;gap:4px;margin-bottom:16px}
.msg-tabs button{
  padding:6px 16px;border-radius:6px;border:1px solid #333;
  background:transparent;color:#999;cursor:pointer;font-size:12px;
  font-weight:500;transition:all .15s;font-family:inherit}
.msg-tabs button:hover{background:#2a2a2a;color:#e0e0e0}
.msg-tabs button.active{background:#2a2a2a;border-color:#666;color:#e0e0e0}

/* Inspect sections */
.insp-method-header{margin-bottom:16px}
.insp-method-header .method-name{font-size:16px;font-weight:700;color:#e0e0e0}
.insp-method-header .method-id{
  font-size:12px;color:#777;margin-left:8px;
  font-family:'SF Mono','Fira Code',monospace}
.insp-msg-title{font-size:14px;font-weight:600;color:#f0f0f0;margin-bottom:10px}
.insp-msg-title .tag{
  font-size:10px;font-weight:500;padding:2px 8px;border-radius:99px;
  margin-left:6px;background:#2a2a2a;color:#999}

/* Field tree */
.field-tree{font-family:'SF Mono','Fira Code','Cascadia Code',monospace;font-size:12px;line-height:1.7}
.field-row{display:flex;align-items:baseline;gap:4px;padding:1px 0;cursor:default}
.field-row:hover{background:rgba(255,255,255,.02)}
.field-row .f-toggle{width:16px;flex-shrink:0;color:#666;cursor:pointer;text-align:center;font-size:10px;user-select:none}
.field-row .f-toggle:hover{color:#aaa}
.field-row .f-type{color:#777;font-style:italic;flex-shrink:0;font-size:11px}
.field-row .f-name{color:#ccc;font-weight:500}
.field-row .f-colon{color:#666;margin:0 3px}
.field-row .f-value.string{color:#ddd}
.field-row .f-value.number{color:#eee}
.field-row .f-value.enum{color:#ccc;font-style:italic}
.field-row .f-value.bool{color:#bbb}
.field-row .f-value.other{color:#aaa;font-style:italic}
.field-row .f-value.truncated{color:#777;font-style:italic}
.field-row .f-brace{color:#666;font-weight:600}
.field-children{margin-left:16px;border-left:1px solid #2a2a2a;padding-left:2px}
.field-children.hidden{display:none}

/* Proxy section */
.proxy-block{margin-top:20px;padding:16px;border-radius:8px;background:#1a1a1a;border:1px solid #333}
.proxy-block .proxy-label{font-size:13px;font-weight:600;color:#aaa;margin-bottom:12px}

/* Blackbox / error */
.blackbox{font-family:'SF Mono','Fira Code',monospace;font-size:11px;color:#ccc;white-space:pre-wrap}

/* Scrim behind inspect when nothing selected */
.empty-inspect{color:#555;text-align:center;padding-top:80px;font-size:14px}

/* === Responsive === */
@media(max-width:700px){
  #sidebar{width:100%!important;min-width:unset;max-width:unset}
  #main{display:none}
  #resize-handle{display:none}
}
</style>
</head>
<body>
<div id="header">
  <div class="logo"><span>🚦</span> Traffic Light</div>
  <span class="badge badge-count" id="req-count">0 requests</span>
  <span class="badge" id="conn-dot"></span>
  <span class="spacer"></span>
  <span class="summary" id="summary-text"></span>
</div>
<div id="toolbar">
  <div class="filter-wrap">
    <span class="search-icon">🔍</span>
    <input type="text" id="filter-input" placeholder="Filter by method, message, RPC..." disabled>
    <span class="filter-count" id="filter-count"></span>
  </div>
  <button id="btn-pause">⏸ Pause</button>
  <button id="btn-follow" class="is-active">↓ Follow</button>
  <div class="sep"></div>
  <button id="btn-clear" class="danger">Clear</button>
</div>
<div id="body">
  <div id="sidebar">
    <div class="list-header"><span class="col-time">Time</span><span class="col-meta">Request</span></div>
    <div class="list-body" id="request-list"></div>
  </div>
  <div id="resize-handle"></div>
  <div id="main">
    <div class="empty-state" id="empty-state">
      <div class="icon">📡</div>
      <p>Waiting for traffic…</p>
    </div>
    <div id="inspect-view" style="display:none;flex-direction:column;flex:1;overflow:hidden">
      <div class="proto-tabs" id="proto-tabs"></div>
      <div id="inspect-content"></div>
    </div>
  </div>
</div>
<script>
(function(){
const $=s=>document.querySelector(s),$$=s=>document.querySelectorAll(s);

// State
let records=[],selReq=null,selProto=0,paused=false,follow=true;
let msgTab='request'; // 'request' | 'response'

// DOM
const listEl=$('#request-list'),inspectView=$('#inspect-view'),
  emptyState=$('#empty-state'),inspectContent=$('#inspect-content'),
  protoTabs=$('#proto-tabs'),filterInp=$('#filter-input'),
  connDot=$('#conn-dot'),reqCount=$('#req-count'),
  filterCount=$('#filter-count'),summaryText=$('#summary-text'),
  btnPause=$('#btn-pause'),btnFollow=$('#btn-follow'),btnClear=$('#btn-clear');

// === WebSocket ===
function connect(){
  const proto=location.protocol==='https:'?'wss':'ws';
  const ws=new WebSocket(proto+'://'+location.host+'/ws');
  ws.onopen=()=>{
    connDot.className='badge badge-ok';connDot.textContent='live';
    filterInp.disabled=false;
  };
  ws.onclose=()=>{
    connDot.className='badge badge-err';connDot.textContent='offline';
    filterInp.disabled=true;setTimeout(connect,2000);
  };
  ws.onmessage=e=>{
    const r=JSON.parse(e.data);
    if(r.type==='record'&&!paused){addRecord(r)}
  };
}

function addRecord(r){
  records.push(r);if(records.length>500)records=records.slice(-500);
  renderRow(r);updateStats();applyFilter();
  if(follow)listEl.scrollTop=listEl.scrollHeight;
  if(!selReq&&records.length===1)selectRequest(0,0);
}

// === Render sidebar row ===
function renderRow(r){
  const el=document.createElement('div');
  el.className='request';el.dataset.idx=records.length-1;

  const methods=r.protos.map(p=>esc(p.method_name||'?')).join('<span class="sep">·</span>');
  const statusCls=r.rpc_status===1?'s-ok':r.rpc_status===0?'s-none':'s-err';
  const statusName=esc(r.rpc_status_name||'Status '+r.rpc_status);

  el.innerHTML=
    '<div class="col-time">'+esc(r.time)+'</div>'+
    '<div class="col-meta">'+
      '<div class="rpc-line">'+
        '<span class="rpc-id">#'+esc(String(r.rpc_id))+'</span>'+
        '<span class="rpc-status '+statusCls+'" title="'+statusName+'">'+esc(String(r.rpc_status))+' '+statusName+'</span>'+
        (r.rpc_handle!=null?'<span class="rpc-handle">h:'+esc(String(r.rpc_handle))+'</span>':'')+
      '</div>'+
      '<div class="methods">'+methods+'</div>'+
    '</div>'+
    '<div class="chevron">▶</div>';

  el.addEventListener('click',e=>{
    const idx=parseInt(el.dataset.idx);
    if(selReq===el){
      el.classList.toggle('expanded');
    }else{
      selectRequest(idx,0);
    }
  });

  listEl.appendChild(el);
}

// === Selection ===
function selectRequest(idx,protoIdx){
  if(selReq)selReq.classList.remove('selected');
  selReq=listEl.querySelector('.request[data-idx="'+idx+'"]');
  if(selReq){selReq.classList.add('selected');selReq.classList.add('expanded')}
  selProto=protoIdx;
  renderInspect(idx,protoIdx);
  emptyState.style.display='none';
  inspectView.style.display='flex';
}

function renderInspect(idx,protoIdx){
  const record=records[idx];
  if(!record)return;
  const proto=record.protos[protoIdx];
  if(!proto)return;

  // Proto tabs
  let ptHtml='';
  for(let i=0;i<record.protos.length;i++){
    const p=record.protos[i];
    ptHtml+='<button class="'+(i===protoIdx?'active':'')+'" data-pi="'+i+'">'+
      esc(p.method_name||'Method '+i)+' <span style="color:#777">#'+esc(String(p.method_value))+'</span></button>';
  }
  protoTabs.innerHTML=ptHtml;
  protoTabs.querySelectorAll('button').forEach(btn=>{
    btn.addEventListener('click',()=>{
      selProto=parseInt(btn.dataset.pi);
      renderInspect(idx,selProto);
    });
  });

  // Content
  let h='';
  // RPC context bar
  h+='<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;padding:8px 12px;background:#1a1a1a;border-radius:6px;font-size:12px">';
  h+='<span style="color:#777">'+esc(record.time)+'</span>';
  h+='<span style="color:#e0e0e0;font-weight:600">RPC #'+esc(String(record.rpc_id))+'</span>';
  const stCls=record.rpc_status===1?'s-ok':record.rpc_status===0?'s-none':'s-err';
  h+='<span class="rpc-status '+stCls+'" style="font-size:11px">'+esc(String(record.rpc_status))+' '+esc(record.rpc_status_name||'')+'</span>';
  if(record.rpc_handle!=null)h+='<span style="color:#666">Handle '+esc(String(record.rpc_handle))+'</span>';
  h+='</div>';

  h+='<div class="insp-method-header">';
  h+='<span class="method-name">'+esc(proto.method_name||'Unknown Method')+'</span>';
  h+='<span class="method-id">#'+esc(String(proto.method_value))+'</span>';
  h+='</div>';

  // Message tabs
  h+='<div class="msg-tabs">';
  h+='<button class="'+(msgTab==='request'?'active':'')+'" data-tab="request">📥 Request</button>';
  h+='<button class="'+(msgTab==='response'?'active':'')+'" data-tab="response">📤 Response</button>';
  h+='</div>';

  const msg=msgTab==='request'?proto.request:proto.response;
  h+=renderMessageDetail(msg);

  // Proxy
  if(proto.proxy){
    h+='<div class="proxy-block">';
    h+='<div class="proxy-label">🔗 Proxy: '+esc(proto.proxy.method_name||'Unknown')+
      ' <span style="color:#777;font-weight:400">#'+esc(String(proto.proxy.method_value))+'</span></div>';
    h+='<div style="margin-bottom:12px">'+renderMessageDetail(proto.proxy.request,'Proxy Request')+'</div>';
    h+=renderMessageDetail(proto.proxy.response,'Proxy Response');
    h+='</div>';
  }

  inspectContent.innerHTML=h;

  // Wire msg tabs
  inspectContent.querySelectorAll('.msg-tabs button').forEach(btn=>{
    btn.addEventListener('click',()=>{
      msgTab=btn.dataset.tab;
      renderInspect(idx,protoIdx);
    });
  });

  // Wire collapsible fields
  inspectContent.querySelectorAll('.f-toggle').forEach(tgl=>{
    tgl.addEventListener('click',e=>{
      e.stopPropagation();
      const children=tgl.closest('.field-row').nextElementSibling;
      if(children&&children.classList.contains('field-children')){
        children.classList.toggle('hidden');
        tgl.textContent=children.classList.contains('hidden')?'▶':'▼';
      }
    });
  });
}

function renderMessageDetail(msg,labelOverride){
  let h='<div class="insp-msg-title">';
  h+=esc(msg.name||'Unknown Message');
  h+='<span class="tag">'+(labelOverride||esc(msg.type))+'</span>';
  h+='</div>';

  if(msg.error&&msg.blackbox){
    h+='<div class="blackbox">'+esc(msg.blackbox)+'</div>';
  }else if(msg.fields&&msg.fields.length>0){
    h+='<div class="field-tree">';
    for(const f of msg.fields)h+=renderField(f);
    h+='</div>';
  }else{
    h+='<div style="color:#666;font-family:monospace;margin-left:4px">— empty —</div>';
  }
  return h;
}

function renderField(field,depth=0){
  const indent=depth*4;
  let h='<div class="field-row" style="padding-left:'+indent+'px">';

  if(field.nested&&Array.isArray(field.value)){
    h+='<span class="f-toggle">▶</span>';
    h+='<span class="f-type">'+(field.label==='repeated'?'repeated ':'')+esc(field.type)+'</span>';
    h+='<span class="f-name">'+esc(field.name)+'</span>';
    h+='<span class="f-brace">{</span>';
    h+='</div>';
    h+='<div class="field-children hidden">';
    for(const sub of field.value)h+=renderField(sub,depth+1);
    h+='<div class="field-row" style="padding-left:'+indent+'px"><span class="f-brace">}</span></div>';
    h+='</div>';
  }else if(field.empty){
    h+='<span class="f-toggle" style="visibility:hidden">·</span>';
    h+='<span class="f-type">'+esc(field.type)+'</span>';
    h+='<span class="f-name">'+esc(field.name)+'</span>';
    h+='<span class="f-brace">{ }</span>';
    h+='</div>';
  }else if(field.truncated){
    h+='<span class="f-toggle" style="visibility:hidden">·</span>';
    h+='<span class="f-type">'+esc(field.type)+'</span>';
    h+='<span class="f-name">'+esc(field.name)+'</span>';
    h+='<span class="f-colon">=</span>';
    h+='<span class="f-value truncated">'+esc(String(field.value))+'</span>';
    h+='</div>';
  }else{
    h+='<span class="f-toggle" style="visibility:hidden">·</span>';
    h+='<span class="f-type">'+(field.label==='repeated'?'repeated ':'')+esc(field.type)+'</span>';
    h+='<span class="f-name">'+esc(field.name)+'</span>';
    h+='<span class="f-colon">=</span>';
    const cls=field.css_class||'other';
    h+='<span class="f-value '+cls+'">'+esc(String(field.value))+'</span>';
    h+='</div>';
  }
  return h;
}

// === Filter ===
function applyFilter(){
  const text=filterInp.value.toLowerCase().trim();let vis=0,total=records.length;
  $$('#request-list .request').forEach((el,i)=>{
    if(!text){el.classList.remove('hidden');vis++;return}
    const r=records[i];if(!r){el.classList.add('hidden');return}
    let m=false;
    const head='#'+r.rpc_id+' '+r.rpc_status+' '+(r.rpc_status_name||'')+' '+(r.rpc_handle!=null?r.rpc_handle:'');
    if(head.includes(text))m=true;
    if(!m)for(const p of r.protos){
      if((p.method_name||'').toLowerCase().includes(text)){m=true;break}
      for(const msg of[p.request,p.response]){
        if((msg.name||'').toLowerCase().includes(text)){m=true;break}
      }
      if(m)break;
      if(p.proxy&&(p.proxy.method_name||'').toLowerCase().includes(text)){m=true;break}
    }
    if(m){el.classList.remove('hidden');vis++}
    else el.classList.add('hidden');
  });
  filterCount.textContent=text?vis+'/'+total:'';
  summaryText.textContent=text?vis+' of '+total+' shown':total+' request'+(total!==1?'s':'');
}

// === Controls ===
filterInp.addEventListener('input',applyFilter);
btnPause.addEventListener('click',()=>{
  paused=!paused;btnPause.classList.toggle('is-active',paused);
  btnPause.innerHTML=paused?'▶ Resume':'⏸ Pause';
});
btnFollow.addEventListener('click',()=>{
  follow=!follow;btnFollow.classList.toggle('is-active',follow);
});
btnClear.addEventListener('click',()=>{
  records=[];listEl.innerHTML='';selReq=null;selProto=0;
  inspectView.style.display='none';emptyState.style.display='flex';
  protoTabs.innerHTML='';inspectContent.innerHTML='';
  updateStats();applyFilter();
});

// === Resize ===
const resizeHandle=$('#resize-handle'),sidebar=$('#sidebar');
let dragging=false,startX=0,startW=0;
resizeHandle.addEventListener('mousedown',e=>{
  dragging=true;startX=e.clientX;startW=sidebar.offsetWidth;
  resizeHandle.classList.add('dragging');
  document.body.style.cursor='col-resize';document.body.style.userSelect='none';
});
document.addEventListener('mousemove',e=>{
  if(!dragging)return;
  const w=Math.max(220,Math.min(startW+e.clientX-startX,window.innerWidth*.55));
  sidebar.style.width=w+'px';
});
document.addEventListener('mouseup',()=>{
  if(!dragging)return;
  dragging=false;resizeHandle.classList.remove('dragging');
  document.body.style.cursor='';document.body.style.userSelect='';
});

// === Keyboard ===
document.addEventListener('keydown',e=>{
  if(e.target.tagName==='INPUT')return;
  if(e.key==='Escape'){filterInp.value='';applyFilter();filterInp.blur()}
  if(e.key==='/'&&e.target===document.body){e.preventDefault();filterInp.focus()}
  if(e.key==='ArrowDown'&&selReq){
    e.preventDefault();
    const next=selReq.nextElementSibling;
    if(next&&next.classList.contains('request')&&!next.classList.contains('hidden')){
      selectRequest(parseInt(next.dataset.idx),selProto);
      next.scrollIntoView({block:'nearest'});
    }
  }
  if(e.key==='ArrowUp'&&selReq){
    e.preventDefault();
    const prev=selReq.previousElementSibling;
    if(prev&&prev.classList.contains('request')&&!prev.classList.contains('hidden')){
      selectRequest(parseInt(prev.dataset.idx),selProto);
      prev.scrollIntoView({block:'nearest'});
    }
  }
  if(e.key==='t'&&e.ctrlKey&&selReq){
    e.preventDefault();msgTab=msgTab==='request'?'response':'request';
    renderInspect(parseInt(selReq.dataset.idx),selProto);
  }
});

// === Helpers ===
function esc(s){
  const d=document.createElement('div');
  d.appendChild(document.createTextNode(String(s)));
  return d.innerHTML;
}
function updateStats(){
  const n=records.length;
  reqCount.textContent=n+' request'+(n!==1?'s':'');
  summaryText.textContent=n+' request'+(n!==1?'s':'');
}

// Init
connect();
})();
</script>
</body>
</html>"""
