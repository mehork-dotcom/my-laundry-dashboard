"""
payment_handler.py — My Laundry Payment System (Beam Integration)
"""

import os, hmac, hashlib, asyncio, json as _json, base64
import httpx
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from firebase_admin import db
import uvicorn

TZ_THAI          = timezone(timedelta(hours=7))
LINE_TOKEN       = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
ADMIN_LINE_ID    = os.environ.get("ADMIN_LINE_USER_ID", "")
DASHBOARD_URL        = "https://mehork-dotcom.github.io/my-laundry-dashboard/"
BEAM_REDIRECT_URL    = DASHBOARD_URL + "?beam_return=1"
PAYMENT_TIMEOUT  = 5   # minutes

# Beam credentials — สองชุด: Playground + Production
# Railway env vars ต้องมีครบทั้ง: BEAM_PLAYGROUND_* และ BEAM_PRODUCTION_*
_BEAM_CREDS = {
    "playground": {
        "merchant_id": os.environ.get("BEAM_PLAYGROUND_MERCHANT_ID", os.environ.get("BEAM_MERCHANT_ID", "")),
        "api_key":     os.environ.get("BEAM_PLAYGROUND_API_KEY",     os.environ.get("BEAM_API_KEY", "")),
        "secret_key":  os.environ.get("BEAM_PLAYGROUND_SECRET_KEY",  os.environ.get("BEAM_SECRET_KEY", "")),
        "base":        "https://playground.api.beamcheckout.com",
    },
    "production": {
        "merchant_id": os.environ.get("BEAM_PRODUCTION_MERCHANT_ID", os.environ.get("BEAM_MERCHANT_ID", "")),
        "api_key":     os.environ.get("BEAM_PRODUCTION_API_KEY",     os.environ.get("BEAM_API_KEY", "")),
        "secret_key":  os.environ.get("BEAM_PRODUCTION_SECRET_KEY",  os.environ.get("BEAM_SECRET_KEY", "")),
        "base":        "https://api.beamcheckout.com",
    },
}
# fallback env (ถ้า Firebase อ่านไม่ได้)
_DEFAULT_BEAM_ENV = os.environ.get("BEAM_ENV", "playground")

async def _get_beam_creds():
    """อ่าน beam_env จาก Firebase → คืน credentials ชุดที่ถูกต้อง"""
    try:
        env_ref = db.reference("laundry_system/settings/beam_env")
        env = env_ref.get() or _DEFAULT_BEAM_ENV
    except Exception:
        env = _DEFAULT_BEAM_ENV
    return env, _BEAM_CREDS.get(env, _BEAM_CREDS["playground"])

app = FastAPI(title="My Laundry Payment API (Beam)")
app.add_middleware(CORSMiddleware,
    allow_origins=["https://mehork-dotcom.github.io","https://liff.line.me",
                   "https://my-laundry-scraper-production.up.railway.app"],
    allow_methods=["GET","POST","DELETE"], allow_headers=["*"])

def now_thai():   return datetime.now(TZ_THAI)
def to_satang(b): return int(round(b * 100))

# ── Points Multipliers ───────────────────────────────────────────────────
DEFAULT_MULTS = {"cold":1,"warm":2,"hot":2,"dryer":1,"p1":2,"p2":2,"p3":2,"p4":2}

def calc_points(machine_id, program, paid, settings):
    rate  = settings.get("points_rate", 10)
    base  = int(paid / rate)
    mults = settings.get("points_multipliers")
    if mults:
        # ใช้ points_multipliers (format ใหม่)
        key  = "dryer" if program == "single" else program
        mult = int(mults.get(key, DEFAULT_MULTS.get(key, 1)))
        if machine_id.startswith("AQUA"):
            mult = max(mult, 2)  # AQUA ได้อย่างน้อย x2
    else:
        # fallback double_point_programs (format เก่า)
        dbl_list = settings.get("double_point_programs", list(DEFAULT_MULTS.keys()))
        is_dbl   = program in dbl_list or machine_id.startswith("AQUA")
        mult     = 2 if is_dbl else 1
    return base * mult

def is_new_member(member): return member.get("total_uses", 0) == 0

# ── Models ───────────────────────────────────────────────────────────────
class CreatePaymentRequest(BaseModel):
    machine_id: str; program: str; user_id: str = "kiosk"; use_credit: bool = False
    custom_price:  float | None = None   # เพิ่มเวลาอบ/เพิ่มรอบซัก
    use_voucher:   bool = False           # ใช้ loyalty voucher ฟรี

class CoinSlotRequest(BaseModel):
    machine_id: str; program: str; user_id: str; price: float

# ════════════════════════════════════════════════════════════════════════
# CREATE PAYMENT LINK
# ════════════════════════════════════════════════════════════════════════
@app.post("/payment/create")
async def create_payment(body: CreatePaymentRequest):
    mid, prog, uid, use_cred = body.machine_id, body.program, body.user_id, body.use_credit

    # ── ข้อ 4: Lock AQUA จนกว่า M4 (Modbus) จะพร้อม ─────────────
    if mid.startswith("AQUA"):
        raise HTTPException(503, "ตู้ AQUA ยังไม่พร้อมรับชำระเงินผ่านแอป — กรุณาใช้เหรียญชั่วคราว")

    # ── Voucher redemption (ถ้า use_voucher=True) ──────────────────
    if body.use_voucher and uid not in (None, "kiosk") and not uid.startswith("phone_"):
        m_type_v = _machine_loyalty_type(mid) or ""
        vr_ref   = db.reference(f"laundry_system/members/{uid}/loyalty_vouchers")
        vmap     = vr_ref.get() or {}
        matched_key = None
        for vk, vdata in vmap.items():
            if not isinstance(vdata, dict): continue
            if vdata.get("used"):           continue
            if vdata.get("type") != m_type_v: continue
            if vdata.get("program") != prog:  continue
            exp_str = vdata.get("expires_at", "")
            try:
                exp = datetime.fromisoformat(exp_str)
                if datetime.now(timezone.utc) > exp: continue
            except Exception: pass
            matched_key = vk
            break
        if not matched_key:
            raise HTTPException(400, "ไม่พบสิทธิ์ฟรีสำหรับโปรแกรมนี้ หรือสิทธิ์หมดอายุแล้ว")
        # mark voucher used
        vr_ref.child(matched_key).update({
            "used": True,
            "used_at": now_thai().isoformat(),
            "used_tx":  str(int(now_thai().timestamp()))
        })
        tx_id = f"V{int(now_thai().timestamp()*1000)}"
        _save_tx(tx_id, mid, prog, uid, price, 0, 0, 0, "free")
        await _process_success(tx_id, mid, prog, uid, 0, price, False)
        return {"status": "free", "tx_id": tx_id, "message": "ใช้สิทธิ์ Loyalty Voucher สำเร็จ"}

    # ── ข้อ 3: Double-pay guard — ป้องกันสร้าง TX ซ้ำสำหรับเครื่องเดียวกัน ──
    existing_tx = db.reference("laundry_system/transactions").get() or {}
    _cutoff = now_thai() - timedelta(minutes=PAYMENT_TIMEOUT + 1)
    for _tx in existing_tx.values():
        if not isinstance(_tx, dict): continue
        if _tx.get("machine_id") != mid: continue
        if _tx.get("status") not in ("pending",): continue
        try:
            _created = datetime.fromisoformat(_tx.get("expire_at","")).replace(tzinfo=None) - timedelta(minutes=PAYMENT_TIMEOUT)
            if _created > _cutoff.replace(tzinfo=None):
                raise HTTPException(409, f"มี transaction ที่รอชำระอยู่แล้วสำหรับ {mid} — กรุณารอหรือยกเลิกก่อน")
        except HTTPException: raise
        except Exception: pass

    # ตรวจสถานะ
    m_data = db.reference(f"laundry_system/machines/{mid}").get() or {}
    status = (m_data.get("status") or "").lower()
    is_w   = mid.startswith("W")
    if is_w and status not in ("available","readytostart",""):
        raise HTTPException(409, f"เครื่อง {mid} ไม่ว่าง (สถานะ: {status})")
    if not is_w and status in ("error","outoforder","unavailable","offline"):
        raise HTTPException(409, f"เครื่อง {mid} ไม่สามารถใช้งานได้")

    # ราคา
    pricing  = db.reference(f"laundry_system/pricing/{mid}").get() or {}
    if body.custom_price and body.custom_price > 0:
        cp = float(body.custom_price)
        # Validate: ต้องเป็นทวีคูณของ 10 และอยู่ในช่วงที่ยอมรับ
        if cp < 10:
            raise HTTPException(400, "custom_price ต้องไม่ต่ำกว่า ฿10")
        if cp % 10 != 0:
            raise HTTPException(400, "custom_price ต้องเป็นทวีคูณของ 10")
        # ตรวจ max จาก Firebase settings
        settings_ref = db.reference("laundry_system/settings")
        _s = settings_ref.get() or {}
        if mid.startswith("AQUA"):
            max_allowed = float(_s.get("aqua_extend_max", 100))
        else:
            max_allowed = float(_s.get("dryer_extend_max", 50))
        if cp > max_allowed:
            raise HTTPException(400, f"custom_price เกินวงเงินสูงสุด ฿{max_allowed:.0f}")
        price = cp
    else:
        price = float(pricing.get(prog, 0))
        if price <= 0: raise HTTPException(400, f"ไม่พบโปรแกรม '{prog}' สำหรับ {mid}")

    settings = db.reference("laundry_system/settings/members").get() or {}
    member   = db.reference(f"laundry_system/members/{uid}").get() or {} if uid != "kiosk" else {}

    # credit
    credit_used = 0.0
    if use_cred and uid != "kiosk":
        credit_used = min(float(member.get("credit_balance",0)), price)

    final = max(0.0, price - credit_used)

    # Free (full credit)
    if final < 1:
        tx_id = f"FREE_{mid}_{now_thai().strftime('%Y%m%d%H%M%S')}"
        _save_tx(tx_id, mid, prog, uid, price, 0, credit_used, 0, "free")
        await _process_success(tx_id, mid, prog, uid, credit_used, price, False)
        return {"status":"free","tx_id":tx_id,"message":"ใช้ credit ครบ เครื่องพร้อมใช้งาน"}

    # Beam Payment Link — อ่าน env + credentials จาก Firebase
    beam_env, beam_creds = await _get_beam_creds()
    _merchant_id = beam_creds["merchant_id"]
    _api_key     = beam_creds["api_key"]
    _beam_base   = beam_creds["base"]
    print(f"🌐 Beam env: {beam_env} | merchant: {_merchant_id[:10]}...")

    tx_id      = f"TX_{mid}_{now_thai().strftime('%Y%m%d%H%M%S')}_{uid[:8]}"
    exp_at     = now_thai() + timedelta(minutes=PAYMENT_TIMEOUT)
    exp_at_utc = exp_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"{_beam_base}/api/v1/payment-links",
            auth=(_merchant_id, _api_key),
            json={
                "order": {
                    "netAmount":   to_satang(final),   # satang (฿60 = 6000)
                    "currency":    "THB",
                    "description": f"My Laundry — {mid} {prog.upper()}",
                    "referenceId": tx_id,
                },
                "linkSettings": {
                    "mobileBanking": {"isEnabled": True},   # nested object — Beam format
                    "qrPromptPay":   {"isEnabled": True},
                },
                "expiresAt":              exp_at_utc,        # UTC ISO 8601 → แก้ Invalid Date
                "redirectUrl":            BEAM_REDIRECT_URL,   # ?beam_return=1 → ปิด tab อัตโนมัติ
                "collectDeliveryAddress": False,
                "collectPhoneNumber":     False,
            })
        print(f"🔍 Beam status: {r.status_code} | response keys: {list(r.json().keys()) if r.status_code in (200,201) else r.text[:300]}")
        if r.status_code in (200,201): print(f"🔍 Beam full response: {r.text[:500]}")

    if r.status_code not in (200,201):
        print(f"❌ Beam {r.status_code}: {r.text[:200]}")
        raise HTTPException(502, f"Beam error: {r.json().get('message','ไม่สามารถสร้างลิ้งค์ได้')}")

    beam = r.json()
    pts  = calc_points(mid, prog, final, settings)
    if is_new_member(member) and uid != "kiosk": pts += 50

    _save_tx(tx_id, mid, prog, uid, price, 0, credit_used, final, "pending",
             beam_link_id=beam.get("id",""), expire_at=exp_at.isoformat())  # CREATE response ใช้ id, GET response ใช้ paymentLinkId
    asyncio.create_task(_expire_tx(tx_id, PAYMENT_TIMEOUT * 60))

    return {"tx_id": tx_id, "payment_url": beam.get("url",""),
            "amount": final, "original": price, "credit_used": credit_used,
            "points_preview": pts, "expire_at": exp_at.isoformat(),
            "machine_id": mid, "program": prog}


# ════════════════════════════════════════════════════════════════════════
# BEAM WEBHOOK
# ════════════════════════════════════════════════════════════════════════
@app.post("/payment/webhook")
async def beam_webhook(request: Request, bg: BackgroundTasks):
    body_bytes = await request.body()

    # Verify HMAC — อ่าน secret key ตาม env ปัจจุบัน
    try:
        _, _creds = await _get_beam_creds()
        _secret = _creds["secret_key"] or ""
    except Exception:
        _secret = ""
    if _secret:
        sig = request.headers.get("x-beam-signature","")
        key_bytes = base64.b64decode(_secret)
        exp = base64.b64encode(hmac.new(key_bytes, body_bytes, hashlib.sha256).digest()).decode()
        if sig != exp:
            print(f"❌ HMAC mismatch | sig={sig[:20]}... exp={exp[:20]}...")
            raise HTTPException(401, "Invalid signature")

    # Payment Links webhook ใช้ event "payment_link.paid"
    ev = request.headers.get("x-beam-event","")
    if ev != "payment_link.paid":
        return {"message": f"event={ev} ignored"}

    body = _json.loads(body_bytes)
    # Payment Links payload: referenceId อยู่ใน order object
    order      = body.get("order", {})
    ref_id     = order.get("referenceId", "")
    paid_satang = order.get("netAmount", 0)
    paid       = paid_satang / 100      # satang → baht
    status     = body.get("status", "") # "PAID"

    if status != "PAID":
        return {"message": f"status={status} ignored"}

    tx_ref  = db.reference(f"laundry_system/transactions/{ref_id}")
    tx_data = tx_ref.get()
    if not tx_data:    return {"message":"not found"}
    if tx_data.get("is_processed"): return {"message":"idempotent"}

    tx_ref.update({"is_processed":True,"paid_at":now_thai().isoformat(),
                   "payment_link_id":body.get("paymentLinkId",""),"paid_amount":paid})
    bg.add_task(_process_success, ref_id, tx_data.get("machine_id"),
                tx_data.get("program"), tx_data.get("user_id"),
                float(tx_data.get("credit_used",0)), paid, True)
    return {"message":"ok"}


# ════════════════════════════════════════════════════════════════════════
# COIN SLOT
# ════════════════════════════════════════════════════════════════════════
@app.post("/payment/coin-slot")
async def coin_slot(body: CoinSlotRequest, bg: BackgroundTasks):
    if not body.user_id or body.user_id == "kiosk":
        raise HTTPException(400, "ต้องมี userId")
    m = db.reference(f"laundry_system/machines/{body.machine_id}").get() or {}
    if (m.get("status") or "").lower() not in ("available","readytostart",""):
        raise HTTPException(409, "เครื่องไม่ว่าง")

    cid = f"COIN_{body.machine_id}_{now_thai().strftime('%Y%m%d%H%M%S')}"
    db.reference(f"laundry_system/coin_claims/{cid}").set({
        "machine_id":body.machine_id,"program":body.program,"user_id":body.user_id,
        "price":body.price,"status":"pending","created_at":now_thai().isoformat(),
        "expire_at":(now_thai()+timedelta(minutes=3)).isoformat()})
    bg.add_task(_poll_coin, cid, body)
    return {"claim_id":cid,"message":"กำลังรอเครื่องเริ่มทำงาน (3 นาที)"}

@app.delete("/payment/coin-slot/{cid}")
async def cancel_coin(cid: str):
    db.reference(f"laundry_system/coin_claims/{cid}").update({"status":"cancelled"})
    return {"message":"ยกเลิกแล้ว"}

@app.get("/payment/coin-slot/{cid}")
async def get_coin(cid: str):
    d = db.reference(f"laundry_system/coin_claims/{cid}").get()
    if not d: raise HTTPException(404,"ไม่พบ claim")
    return {"claim_id":cid,"status":d.get("status"),"points":d.get("points_earned",0)}

@app.get("/payment/status/{tx_id}")
async def payment_status(tx_id: str):
    tx = db.reference(f"laundry_system/transactions/{tx_id}").get()
    if not tx: raise HTTPException(404,"not found")
    return {"tx_id":tx_id,"status":tx.get("status"),"is_processed":tx.get("is_processed",False),
            "machine_id":tx.get("machine_id"),"program":tx.get("program"),
            "paid_amount":tx.get("paid_amount",0),"points":tx.get("points_earned",0)}


# ════════════════════════════════════════════════════════════════════════
# PROCESSING HELPERS
# ════════════════════════════════════════════════════════════════════════
def _machine_loyalty_type(mid: str):
    """แปลง machine_id → loyalty type"""
    if mid.startswith("W"):    return "washer"
    if mid.startswith("D"):    return "dryer"
    if mid.startswith("AQUA"): return "aqua"
    return None

async def _award_loyalty(uid: str, m_type: str, settings: dict):
    """เพิ่ม stamp ตาม machine type → ตรวจ milestone → reward"""
    if not uid or uid == "kiosk": return
    if not _feature(f"loyalty_{m_type}", True): return

    loyalty_cfg = settings.get(f"loyalty_{m_type}", {})
    milestones  = loyalty_cfg.get("milestones", [])

    # Default milestones ถ้าไม่ได้ตั้งค่า
    if not milestones:
        _defaults = {
            "washer": [{"at":8,"type":"free","program":"cold"},{"at":16,"type":"free","program":"warm"}],
            "dryer":  [{"at":6,"type":"free","program":"single"},{"at":12,"type":"free","program":"single"}],
            "aqua":   [{"at":6,"type":"free","program":"p4"},{"at":12,"type":"free","program":"p2"},{"at":20,"type":"free","program":"p1"}],
        }
        milestones = _defaults.get(m_type, [])

    stamp_key = f"loyalty_{m_type}"
    mr        = db.reference(f"laundry_system/members/{uid}")
    cur_data  = mr.get() or {}
    cur_stamp = int(cur_data.get(stamp_key, 0)) + 1
    mr.update({stamp_key: cur_stamp})

    if not milestones: return
    milestones = sorted(milestones, key=lambda x: int(x.get("at", 0)))
    period     = int(milestones[-1].get("at", 10))    # loop period = last milestone
    effective  = cur_stamp % period or period          # position within current cycle

    hit = next((m for m in milestones if int(m.get("at",0)) == effective), None)
    if not hit: return

    type_th    = {"washer":"ซักผ้า","dryer":"อบผ้า","aqua":"AQUA"}.get(m_type, m_type)
    prog_names = {"cold":"น้ำเย็น","warm":"น้ำอุ่น","hot":"น้ำร้อน","single":"อบผ้า",
                  "p1":"P1-ซักอบ พิเศษ","p2":"P2-ซักอบ มาตรฐาน","p3":"P3-ซักน้ำอุ่น","p4":"P4-ซักน้ำเย็น"}

    print(f"🎟️ Milestone! uid={uid[:8]} {m_type} stamp={cur_stamp} (effective={effective}) → {hit}")

    reward_type = hit.get("type", "free")
    reward_prog = hit.get("program", "")
    reward_val  = float(hit.get("value", 0))
    at_n        = int(hit.get("at", 0))
    cycle_n     = (cur_stamp - 1) // period + 1

    if reward_type == "credit" and reward_val > 0:
        cur_credit = float((mr.get() or {}).get("credit_balance", 0))
        mr.update({"credit_balance": cur_credit + reward_val})
        _msg = (f"🎉 ครบ {at_n} ครั้ง{type_th} (รอบที่ {cycle_n})!\n"
                f"ได้รับ credit ฿{reward_val:.0f}\nหักอัตโนมัติในการจ่ายครั้งถัดไป")
        await _line_push(uid, _msg)

    elif reward_type == "free" and reward_prog:
        exp = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        db.reference(f"laundry_system/members/{uid}/loyalty_vouchers").push({
            "type": m_type, "program": reward_prog,
            "earned_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": exp, "used": False,
            "milestone_at": at_n, "cycle": cycle_n,
        })
        _pname = prog_names.get(reward_prog, reward_prog.upper())
        _msg2  = (f"🎉 ครบ {at_n} ครั้ง{type_th} (รอบที่ {cycle_n})!\n"
                  f"ได้สิทธิ์ฟรี: {_pname}\n"
                  f"ใช้ได้ภายใน 30 วัน — กด 'ใช้สิทธิ์ฟรี' ในแอป 🧺")
        await _line_push(uid, _msg2)


async def _process_success(tx_id, mid, prog, uid, credit_used, paid, background=True):
    tx_ref = db.reference(f"laundry_system/transactions/{tx_id}")
    try:
        m      = db.reference(f"laundry_system/machines/{mid}").get() or {}
        status = (m.get("status") or "").lower()
        is_w   = mid.startswith("W")
        ok     = status in ("available","readytostart") or (not is_w and status in ("busy","inuse"))

        if ok:
            db.reference(f"laundry_system/commands/{mid}").set({
                "action":"start","status":"pending","requested_at":now_thai().isoformat(),
                "requested_by":"payment_system","tx_id":tx_id,"program":prog})
            settings = db.reference("laundry_system/settings/members").get() or {}
            member   = db.reference(f"laundry_system/members/{uid}").get() or {} if uid!="kiosk" else {}
            pts      = calc_points(mid, prog, paid, settings)
            new_m    = is_new_member(member) and uid != "kiosk"
            if new_m: pts += 50
            tx_ref.update({"status":"started","points_earned":pts})

            if uid and uid != "kiosk":
                await _update_pts(uid, mid, prog, paid, pts, settings)
            # Loyalty stamp per machine type
            m_type = _machine_loyalty_type(mid)
            if m_type:
                await _award_loyalty(uid, m_type, settings)
            if credit_used > 0:
                mr = db.reference(f"laundry_system/members/{uid}")
                cur = float((mr.get() or {}).get("credit_balance",0))
                mr.update({"credit_balance": max(0, cur - credit_used)})
            # ตรวจ multiplier จาก settings (รองรับ format ใหม่ points_multipliers)
            _mults = settings.get("points_multipliers", DEFAULT_MULTS)
            _key   = "dryer" if prog == "single" else prog
            _mult  = int(_mults.get(_key, DEFAULT_MULTS.get(_key, 1)))
            lbl = f" (x{_mult}🎉)" if _mult > 1 else ""
            _mnames = {'W1':'เครื่องซัก 1','W2':'เครื่องซัก 2','W3':'เครื่องซัก 3','W4':'เครื่องซัก 4',
                           'D5':'เครื่องอบ 5','D6':'เครื่องอบ 6','D7':'เครื่องอบ 7','D8':'เครื่องอบ 8'}
            _pnames = {'cold':'น้ำเย็น','warm':'น้ำอุ่น','hot':'น้ำร้อน','single':'อบผ้า',
                           'p1':'P1-ซักอบ พิเศษ','p2':'P2-ซักอบ มาตรฐาน',
                           'p3':'P3-ซักน้ำอุ่น','p4':'P4-ซักน้ำเย็น'}
            _mname = _mnames.get(mid, mid)
            _pname = _pnames.get(prog, prog.upper())
            msg  = f"✅ ชำระเงินสำเร็จ!\n"
            msg += f"━━━━━━━━━━━━━━━\n"
            msg += f"🏠 {_mname}\n"
            msg += f"🌀 โปรแกรม: {_pname}\n"
            msg += f"💰 ยอดชำระ: ฿{paid:.0f}\n"
            msg += f"⭐ แต้มที่ได้: +{pts} แต้ม{lbl}\n"
            if new_m and _feature("welcome_bonus"): msg += f"🎁 Welcome bonus: +{WELCOME_BONUS} แต้ม\n"
            msg += f"━━━━━━━━━━━━━━━\n"
            msg += f"⚠️ กรุณากดปุ่ม START ที่หน้าเครื่อง"
            await _line_push(uid, msg)
            print(f"✅ Payment {tx_id} → start {mid} +{pts}pts")
        else:
            if uid and uid != "kiosk" and paid > 0:
                mr  = db.reference(f"laundry_system/members/{uid}")
                cur = float((mr.get() or {}).get("credit_balance",0))
                mr.update({"credit_balance": cur + paid})
                tx_ref.update({"status":"credited","credit_amount":paid})
                await _line_push(uid, f"⚠️ เครื่อง {mid} ไม่พร้อม\nเพิ่ม credit ฿{paid:.0f} ให้แล้ว")
            else:
                tx_ref.update({"status":"failed_no_credit"})
            await _admin_alert(f"⚠️ จ่ายแล้วแต่เครื่องไม่พร้อม!\nTX:{tx_id} เครื่อง:{mid}({status})\nยอด:฿{paid:.0f}")
            print(f"⚠️ Payment {tx_id} → {mid} ไม่พร้อม → credit")
    except Exception as e:
        tx_ref.update({"status":"error","error_log":str(e)[:200]})
        await _admin_alert(f"🔴 Payment error!\nTX:{tx_id}\n{str(e)[:80]}")
        print(f"❌ _process_success: {e}")


REFERRAL_PTS   = 100
WELCOME_BONUS  = 150

def _feature(name: str, default: bool = True) -> bool:
    """อ่าน feature flag จาก Firebase settings/features/{name}"""
    try:
        v = db.reference(f"laundry_system/settings/features/{name}").get()
        return bool(v) if v is not None else default
    except:
        return default

async def _update_pts(uid, mid, prog, paid, pts, settings):
    try:
        mr  = db.reference(f"laundry_system/members/{uid}")
        m   = mr.get() or {}
        old_uses = int(m.get("total_uses", 0))
        new_uses = old_uses + 1
        stamp    = int(m.get("loyalty_stamps", 0))
        if new_uses % int(settings.get("loyalty_every", 10)) == 0: stamp += 1
        mr.update({"points": int(m.get("points", 0)) + pts, "total_uses": new_uses,
                   "total_spent": float(m.get("total_spent", 0)) + paid,
                   "loyalty_stamps": stamp, "last_use_at": now_thai().isoformat(),
                   "user_id": uid, **({} if m else {"joined_at": now_thai().isoformat()})})

        # ── Tier upgrade notification ─────────────────────────────────
        TIER_MAP = {21: "⭐ Silver", 51: "💎 Gold"}
        if new_uses in TIER_MAP and _feature("tier_notifications"):
            _tier = TIER_MAP[new_uses]
            _perks = {"⭐ Silver": "แต้ม x1.5 + ฟรี Cold เมื่อครบสแตมป์",
                      "💎 Gold":   "แต้ม x2 + ฟรี Warm + Birthday bonus +300 แต้ม"}
            await _line_push(uid,
                f"🎉 ยินดีด้วย! อัปเกรดเป็น {_tier}!\n"
                f"ใช้บริการครบ {new_uses} ครั้งแล้ว\n"
                f"สิทธิ์ใหม่: {_perks.get(_tier,'')}\n"
                f"ขอบคุณที่เลือกใช้ My Laundry 🧺")

        # ── Referral bonus: ให้เมื่อ referee ใช้ครั้งแรก ──────────────
        if old_uses == 0 and _feature("referral"):
            referrer_uid = m.get("referral_by")
            if referrer_uid and referrer_uid != uid:
                # ให้แต้ม referee (ตัวเอง)
                mr.update({"points": int((mr.get() or {}).get("points", 0)) + REFERRAL_PTS,
                           "referral_activated": True})
                # ให้แต้ม referrer
                rr = db.reference(f"laundry_system/members/{referrer_uid}")
                rr_data = rr.get() or {}
                rr.update({"points": int(rr_data.get("points", 0)) + REFERRAL_PTS})
                await _line_push(uid,
                    f"🎉 ยินดีต้อนรับ! คุณได้รับ +{REFERRAL_PTS} แต้ม\nจาก Referral bonus (ใช้ครั้งแรกแล้ว)")
                await _line_push(referrer_uid,
                    f"🎉 เพื่อนของคุณใช้งานแล้ว!\nได้รับ +{REFERRAL_PTS} แต้ม Referral bonus")
                print(f"🎁 Referral activated: {uid[:8]} ← {referrer_uid[:8]} → each +{REFERRAL_PTS}pts")

    except Exception as e: print(f"⚠️ _update_pts: {e}")


async def _poll_coin(cid: str, body: CoinSlotRequest):
    ref = db.reference(f"laundry_system/coin_claims/{cid}")
    for _ in range(36):
        await asyncio.sleep(5)
        c = ref.get()
        if not c or c.get("status") in ("cancelled","done"): return
        m  = db.reference(f"laundry_system/machines/{body.machine_id}").get() or {}
        st = (m.get("status") or "").lower()
        if st in ("busy","inuse","running"):
            settings = db.reference("laundry_system/settings/members").get() or {}
            member   = db.reference(f"laundry_system/members/{body.user_id}").get() or {}
            pts = calc_points(body.machine_id, body.program, body.price, settings)
            if is_new_member(member): pts += 50
            await _update_pts(body.user_id, body.machine_id, body.program, body.price, pts, settings)
            ref.update({"status":"done","points_earned":pts})
            await _line_push(body.user_id, f"🎉 บันทึกแต้มสำเร็จ! +{pts} แต้ม\nเครื่อง: {body.machine_id}")
            return
    c = ref.get() or {}
    if c.get("status") == "pending": ref.update({"status":"timeout"})


async def _expire_tx(tx_id, delay):
    await asyncio.sleep(delay)
    try:
        ref = db.reference(f"laundry_system/transactions/{tx_id}")
        d   = ref.get()
        if d and d.get("status") == "pending":
            ref.update({"status":"expired"}); print(f"⏰ TX {tx_id} expired")
    except: pass

def _save_tx(tx_id, mid, prog, uid, price, disc, cred, paid, status, beam_link_id="", expire_at=""):
    db.reference(f"laundry_system/transactions/{tx_id}").set({
        "machine_id":mid,"program":prog,"user_id":uid,"price":price,
        "discount":disc,"credit_used":cred,"paid_amount":paid,"status":status,
        "is_processed":False,"beam_link_id":beam_link_id,"charge_id":None,
        "error_log":None,"points_earned":0,"created_at":now_thai().isoformat(),
        "paid_at":None,"expire_at":expire_at})


async def _line_push(uid, text):
    # skip kiosk และ phone members — ไม่มี LINE user ID
    if not LINE_TOKEN or not uid or uid == "kiosk" or uid.startswith("phone_"): return
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post("https://api.line.me/v2/bot/message/push",
                headers={"Authorization":f"Bearer {LINE_TOKEN}","Content-Type":"application/json"},
                json={"to":uid,"messages":[{"type":"text","text":text}]})
    except Exception as e: print(f"⚠️ LINE push: {e}")


async def _admin_alert(text):
    if not LINE_TOKEN: return
    try:
        snap   = db.reference("laundry_system/admin_users").get() or {}
        admins = [k for k,v in snap.items() if isinstance(v,dict) and v.get("active",True)]
        if not admins and ADMIN_LINE_ID: admins = [ADMIN_LINE_ID]
        for a in admins: await _line_push(a, f"💳 My Laundry\n\n{text}")
    except Exception as e: print(f"⚠️ admin_alert: {e}")


def start_payment_server():
    """รัน FastAPI ใน background thread (ไม่ใช้ uvloop เพื่อไม่ชนกับ Playwright)"""
    import threading

    # ตรวจ env vars ที่จำเป็น
    # ตรวจ credentials อย่างน้อย 1 ชุด (playground หรือ production)
    missing = []
    if not (os.environ.get("BEAM_PLAYGROUND_MERCHANT_ID") or os.environ.get("BEAM_MERCHANT_ID")):
        missing.append("BEAM_PLAYGROUND_MERCHANT_ID (or BEAM_MERCHANT_ID)")
    if not (os.environ.get("BEAM_PLAYGROUND_API_KEY") or os.environ.get("BEAM_API_KEY")):
        missing.append("BEAM_PLAYGROUND_API_KEY (or BEAM_API_KEY)")
    if not (os.environ.get("BEAM_PLAYGROUND_SECRET_KEY") or os.environ.get("BEAM_SECRET_KEY")):
        missing.append("BEAM_PLAYGROUND_SECRET_KEY (or BEAM_SECRET_KEY)")
    if missing:
        print(f"⚠️ Payment server: env vars ยังไม่ได้ตั้งค่า: {', '.join(missing)}")
    else:
        _env = _DEFAULT_BEAM_ENV
        _creds = _BEAM_CREDS.get(_env, _BEAM_CREDS["playground"])
        env_label = "🚀 Production" if _env == "production" else "🧪 Playground"
        print(f"✅ Beam credentials: OK | {env_label} | Merchant: {_creds['merchant_id'][:10]}...")
        print(f"   API: {_creds['base']}")

    port = int(os.environ.get("PORT", 8000))
    def run():
        uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning",
                    loop="asyncio", access_log=False)
    t = threading.Thread(target=run, daemon=True, name="payment-server")
    t.start()
    print(f"💳 Payment server (Beam) เริ่มทำงานที่ port {port}")


if __name__ == "__main__":
    import firebase_admin
    from firebase_admin import credentials
    cred = credentials.Certificate(_json.loads(os.environ["FIREBASE_CONFIG"]))
    firebase_admin.initialize_app(cred, {"databaseURL": os.environ["DATABASE_URL"]})
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
