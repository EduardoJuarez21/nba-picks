import json
import math
import os
import sys
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from nba_utils import versioned_path
try:
    from src.services import _db_put_picks, _db_put_picks_rows
except Exception:
    def _db_put_picks(*_args, **_kwargs):
        return None

    def _db_put_picks_rows(*_args, **_kwargs):
        return 0

LEAGUE = os.getenv("LEAGUE", "nba")
DAY = os.getenv("DAY", "").strip()
if not DAY:
    raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")

CONTEXT_PATH = os.getenv("OUT_PATH_FEATURES", versioned_path(LEAGUE, "features", "ready_context", DAY))
OUT_PATH = os.getenv("OUT_PATH_PRED", versioned_path(LEAGUE, "predictions", "predictions", DAY))
OUT_TSV = os.getenv("OUT_TSV", f"./out/{LEAGUE}/picks_{DAY}.tsv")

# ===== thresholds =====
# P-only defaults (puedes override con .env)
P_MIN = float(os.getenv("P_MIN", "0.55"))
ODDS_MIN = float(os.getenv("ODDS_MIN", "1.30"))
EV_MIN = float(os.getenv("EV_MIN", "0.00"))  # informativo (fallback)
RECENT_WEIGHT = float(os.getenv("NBA_RECENT_WEIGHT", "0.20"))

# ===== market-specific thresholds (override con .env) =====
# Subido de 0.52: NBA home win rate ~50%; apostar con 52% es prácticamente azar
P_MIN_ML = float(os.getenv("P_MIN_ML", "0.55"))
P_MIN_TOTAL = float(os.getenv("P_MIN_TOTAL", "0.56"))
P_MIN_SPREAD = float(os.getenv("P_MIN_SPREAD", "0.53"))  # más estricto por leak (SPREAD)
P_MIN_TOTAL_OVER = float(os.getenv("P_MIN_TOTAL_OVER", "0.58"))
# 0.58: con sigma=16 requiere ~4.5pts de ventaja sobre la línea — EV_MIN_TOTAL=0.06 es el filtro principal
P_MIN_TOTAL_UNDER = float(os.getenv("P_MIN_TOTAL_UNDER", "0.58"))

# CRÍTICO: EV negativo significa aceptar picks matemáticamente perdedores. NBA es ultra-sharp.
# ML subido a 0.04 (requiere edge real sobre el vig)
EV_MIN_ML = float(os.getenv("EV_MIN_ML", "0.04"))
# TOTAL subido a 0.06: mayor incertidumbre en el modelo de totales exige más margen
EV_MIN_TOTAL = float(os.getenv("EV_MIN_TOTAL", "0.06"))
# Subido de 0.00: con EV=0 simplemente estás pagando el vig del bookmaker
EV_MIN_SPREAD = float(os.getenv("EV_MIN_SPREAD", "0.02"))

EDGE_MIN = float(os.getenv("EDGE_MIN", "0.00"))
# NBA ML requiere edge mínimo del 5% — edges menores contra Pinnacle son ruido
EDGE_MIN_ML = float(os.getenv("EDGE_MIN_ML", "0.05"))
EDGE_MIN_SPREAD = float(os.getenv("EDGE_MIN_SPREAD", "0.02"))
EDGE_MIN_TOTAL_OVER = float(os.getenv("EDGE_MIN_TOTAL_OVER", "0.03"))
# Subido de 0.00 a 0.02: consistencia con OVER — sin edge mínimo se paga solo el vig
EDGE_MIN_TOTAL_UNDER = float(os.getenv("EDGE_MIN_TOTAL_UNDER", "0.02"))

# ===== Pinnacle ML divergence gate =====
# Edge > 0.12 contra Pinnacle en ML => el modelo probablemente está equivocado,
# no encontrando valor real. Pinnacle es el book más sharp del mercado y
# sus líneas son la referencia de mercado. Un edge tan alto suele indicar
# que el modelo sobreestima la prob de un equipo por info que ya está en el precio.
EDGE_ML_SUSPICION = float(os.getenv("NBA_EDGE_ML_SUSPICION", "0.08"))
# Edge > 0.25 en TOTAL contra Pinnacle => el modelo probablemente no está capturando
# info de lesiones/lineup. Un 25% de edge en totales es señal de fallo del modelo.
EDGE_TOTAL_SUSPICION = float(os.getenv("NBA_EDGE_TOTAL_SUSPICION", "0.25"))

TOTAL_HIGH_LINE = float(os.getenv("TOTAL_HIGH_LINE", "238.0"))
P_MIN_TOTAL_HIGH = float(os.getenv("P_MIN_TOTAL_HIGH", "0.72"))
TOTAL_HIGH_SIGMA_MULT = float(os.getenv("TOTAL_HIGH_SIGMA_MULT", "1.15"))
TOTAL_HIGH_AUTO_LEAN = bool(int(os.getenv("TOTAL_HIGH_AUTO_LEAN", "0")))

BIG_SPREAD_ABS = float(os.getenv("BIG_SPREAD_ABS", "10.0"))

STAKE_CAP_TOTAL = float(os.getenv("STAKE_CAP_TOTAL", "0.15"))
STAKE_CAP_SPREAD = float(os.getenv("STAKE_CAP_SPREAD", "0.10"))
STAKE_CAP_LEAN = float(os.getenv("STAKE_CAP_LEAN", "0.10"))
STAKE_CAP_SPREAD_INJ_SEV1 = float(os.getenv("STAKE_CAP_SPREAD_INJ_SEV1", "0.10"))

# ===== consensus =====
MIN_BOOKS_BET = int(os.getenv("MIN_BOOKS_BET", "1"))
MIN_BOOKS_LEAN = int(os.getenv("MIN_BOOKS_LEAN", "1"))

# ===== injuries tuning (spread conditional gate) =====
INJ_SPREAD_SEV1_P_ADD = float(os.getenv("INJ_SPREAD_SEV1_P_ADD", "0.05"))

# ===== degraded mode (cuando INJ_UNKNOWN) =====
# Esto evita que se "descarten" todos los picks cuando INJ_UNKNOWN es 100%.
DEG_INJ_ODDS_MIN = float(os.getenv("DEG_INJ_ODDS_MIN", "1.85"))
DEG_INJ_P_MIN = float(os.getenv("DEG_INJ_P_MIN", "0.70"))
DEG_INJ_EV_MIN = float(os.getenv("DEG_INJ_EV_MIN", "0.10"))
DEG_INJ_STAKE_CAP = float(os.getenv("DEG_INJ_STAKE_CAP", "0.05"))

# ===== calibration params =====
# margen esperado ~ Normal(mu=exp_margin, sigma=MARGIN_SIGMA)
# total esperado  ~ Normal(mu=exp_total,  sigma=TOTAL_SIGMA)
MARGIN_SIGMA = float(os.getenv("NBA_MARGIN_SIGMA", "13.5"))
TOTAL_SIGMA = float(os.getenv("NBA_TOTAL_SIGMA", "16.0"))
PACE_BASELINE = float(os.getenv("NBA_PACE_BASELINE", "100.0"))
SIGMA_MIN_MARGIN = float(os.getenv("NBA_SIGMA_MIN_MARGIN", "10.0"))
SIGMA_MAX_MARGIN = float(os.getenv("NBA_SIGMA_MAX_MARGIN", "15.0"))
SIGMA_MIN_TOTAL = float(os.getenv("NBA_SIGMA_MIN_TOTAL", "13.0"))
SIGMA_MAX_TOTAL = float(os.getenv("NBA_SIGMA_MAX_TOTAL", "19.0"))

# ===== sigma inflation by sample size =====
SIGMA_SAMPLE_REF = int(os.getenv("NBA_SIGMA_SAMPLE_REF", "20"))
SIGMA_SAMPLE_CAP = float(os.getenv("NBA_SIGMA_SAMPLE_CAP", "1.35"))

# ===== pace-matchup params =====
# Cuando dos equipos de ritmo alto se enfrentan, el partido genera más
# posesiones de lo que sugiere el promedio simple de paces.
# PACE_INTERACTION: por cada punto que el avg_pace supera PACE_BASELINE,
#   se suman (PACE_INTERACTION * exceso) posesiones extra.
#   Ej: avg_pace=103 → bonus = (103-100)*0.5 = +1.5 poss → exp_total sube ~3 pts
PACE_INTERACTION = float(os.getenv("NBA_PACE_INTERACTION", "0.5"))
# PACE_HIGH_THRESHOLD / SIGMA_MULT: si el ritmo efectivo supera el umbral,
#   se amplía sigma_total para reflejar mayor incertidumbre en el marcador.
PACE_HIGH_THRESHOLD = float(os.getenv("NBA_PACE_HIGH_THRESHOLD", "100.5"))
PACE_HIGH_SIGMA_MULT = float(os.getenv("NBA_PACE_HIGH_SIGMA_MULT", "1.12"))

# ===== TSV columns =====
# Nota: este header está alineado con lo que realmente escribimos abajo,
# para que Excel/DB NO corrán columnas.
TSV_COLS = [
    "run_id",
    "created_at_utc",
    "kickoff_utc",
    "home",
    "away",
    "match",
    "match_id",
    "market",
    "pick",
    "line",
    "odds",
    "book",
    "books_count",
    # placeholders "tipo fútbol" (se dejan en blanco pero conservan esquema)
    "lambda_home",
    "lambda_away",
    "p_home",
    "p_draw",
    "p_away",
    # NBA model outputs
    "exp_home",
    "exp_away",
    "exp_total",
    "exp_margin",
    "stake_u",
    "p_hat",
    "p_implied",
    "edge",
    "ev",
    "hh_games",
    "aa_games",
    "decision",
    "flags",
    "bet",
    "result",
]
TSV_HEADER = "\t".join(TSV_COLS)


def _sanitize_utc_kickoff(raw: str) -> str:
    """
    Corrige strings mal armados tipo: 2026-01-06T00:00:00T00:00:00Z
    y deja un ISO-8601 razonable.
    """
    if not raw:
        return ""
    s = str(raw).strip()
    if s.count("T") > 1:
        parts = s.split("T")
        s = parts[0] + "T" + parts[-1]
    return s


def _parse_et_status(date_str: str, status_str: str) -> str:
    """
    Intenta parsear '7:30 pm ET' usando el día actual para generar un UTC válido.
    Asume ET ~ UTC-5 (Standard) en invierno, o UTC-4 en verano.
    Por simplicidad y dado que estamos en FEB, usaremos +5h (EST).
    """
    if not status_str or "ET" not in status_str:
        return ""
    
    # "7:30 pm ET" -> "7:30 pm"
    s = status_str.replace("ET", "").strip()
    try:
        t = datetime.strptime(s, "%I:%M %p") # 19:30
        d = datetime.strptime(date_str, "%Y-%m-%d")
        # Combina fecha y hora local ET
        dt_et = d.replace(hour=t.hour, minute=t.minute, second=0)
        # EST = UTC-5 -> UTC = EST+5
        dt_utc = dt_et + timedelta(hours=5)
        return dt_utc.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return ""


def _stake_units(p_hat: float | None, odds: float | None) -> float:
    """
    Criterio de Kelly para Stake Profesional:
    f = (p(b+1) - 1) / b  = (p*odds - 1) / (odds - 1)
    Se aplica fracción de Kelly (KELLY_FRACTION) y CAP.
    """
    if not isinstance(p_hat, (int, float)) or not isinstance(odds, (int, float)):
        return 0.0
    
    if odds <= 1.0 or p_hat <= 0:
        return 0.0

    b = odds - 1.0
    # Fórmula Kelly: % del bankroll a apostar
    f_star = (p_hat * odds - 1.0) / b
    
    if f_star <= 0:
        return 0.0

    # Kelly Fraccional (Conservador)
    stake_pct = f_star * KELLY_FRACTION
    
    # Cap de seguridad
    stake_pct = min(stake_pct, MAX_STAKE)
    
    # Normalizamos salida para compatibilidad con sistema (0.20 era max).
    # Asumimos 1u = 2% bank. Retornamos "unidades relativas".
    return round(stake_pct * 10.0, 3)


# Para compatibilidad (ya no se usan, pero los dejo por si tu .env los trae)
_ = float(os.getenv("NBA_MARGIN_SCALE", "13.0"))
_ = float(os.getenv("NBA_TOTAL_SCALE", "15.0"))


def _read_json(path: str) -> Any:
    if not Path(path).is_file():
        return None
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _ev_decimal(p_win: float, odds: float) -> float:
    p_win = max(0.0, min(1.0, p_win))
    p_lose = 1.0 - p_win
    return p_win * (odds - 1.0) - p_lose


def _p_implied_decimal(odds: float) -> float:
    if not odds or odds <= 0:
        return 0.0
    return 1.0 / odds


def _p_implied_pair(odds_a: float | None, odds_b: float | None) -> Tuple[float | None, float | None]:
    """
    Quita vig normalizando el par (solo si el par representa el MISMO mercado):
      pA = (1/oddsA) / ((1/oddsA)+(1/oddsB))
      pB = (1/oddsB) / ((1/oddsA)+(1/oddsB))
    """
    if not odds_a or not odds_b or odds_a <= 0 or odds_b <= 0:
        return None, None
    ia = 1.0 / odds_a
    ib = 1.0 / odds_b
    s = ia + ib
    if s <= 0:
        return None, None
    return ia / s, ib / s


def _as_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _rest_penalty(rest_flags: Dict[str, Any] | None) -> float:
    if not rest_flags:
        return 0.0
    p = 0.0
    if rest_flags.get("b2b"):
        p += 1.5
    if rest_flags.get("three_in_four"):
        p += 1.0
    return p
# =========================
# TOTAL calibrator (NBA)
# =========================
# =========================
# Tipster Params (Kelly & Impact)
# =========================
KEY_PLAYERS_PATH = os.getenv("NBA_KEY_PLAYERS_PATH", "./data/nba_player_impact.json")

# Impacto en puntos por Tier (bidireccional: ofensiva propia baja, defensa empeora -> rival sube)
IMPACT_TIERS = {
    "S+": 5.5,  # MVP level (Jokic, Luka)
    "S": 4.0,   # All-NBA 1st/2nd team
    "A": 2.5,   # All-Star
    "B": 1.5,   # Starter sólido / 6th man clave
}

# Kelly Fraction (conservador para evitar ruina)
# Bajado de 0.25 (K/4) a 0.20 (K/5): NBA es el mercado más sharp de deportes americanos
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.20"))  # K/5
MAX_STAKE = float(os.getenv("MAX_STAKE", "0.05")) # Cap duro del 5% del bankroll

TOTAL_CAL_PATH = os.getenv("NBA_TOTAL_CAL_PATH", "./scripts/calib/nba_total_cal_v1.json")
TOTAL_CAL_USE = False  # desactivado: origen de training data incierto (no se sabe si usó Pinnacle)

def _load_total_calibrator() -> dict | None:
    if not TOTAL_CAL_USE:
        return None
    p = Path(TOTAL_CAL_PATH)
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _load_key_players() -> dict:
    p = Path(KEY_PLAYERS_PATH)
    if not p.is_file():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

TOTAL_CAL = _load_total_calibrator()
KEY_PLAYERS = _load_key_players()


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)

def _calibrate_total_p(
    z: float,
    odds: float,
    line: float,
    pick: str,  # "OVER"|"UNDER"
) -> float:
    """
    Calibración aprendida:
      p = sigmoid(bias + w_z*z + w_logodds*log(odds) + w_line*((line-225)/10) + w_over*I[OVER])

    Si no hay archivo de calibración, fallback a p_raw = Phi(z).
    """
    p_raw = _norm_cdf(z)
    if not TOTAL_CAL:
        return p_raw

    w = (TOTAL_CAL.get("weights") or {})
    bias = float(w.get("bias", 0.0))
    w_z = float(w.get("z", 1.0))
    w_logodds = float(w.get("log_odds", 0.0))
    w_line = float(w.get("line_norm", 0.0))
    w_over = float(w.get("pick_is_over", 0.0))

    log_odds = math.log(max(1.01, float(odds)))
    line_norm = (float(line) - 225.0) / 10.0
    is_over = 1.0 if str(pick) == "OVER" else 0.0

    score = bias + (w_z * float(z)) + (w_logodds * log_odds) + (w_line * line_norm) + (w_over * is_over)
    p = _sigmoid(score)

    # clamp seguridad
    return max(0.0001, min(0.9999, p))


def _apply_rest_bidirectional(
    exp_home: float,
    exp_away: float,
    home_rest: Dict[str, Any] | None,
    away_rest: Dict[str, Any] | None,
) -> Tuple[float, float]:
    """
    Tipster logic: la fatiga afecta tu ofensiva (baja puntos) y tu defensa (sube puntos del rival).
    Repartimos: 60% al scoring propio, 40% a concedidos.
    """
    ph = _rest_penalty(home_rest)
    pa = _rest_penalty(away_rest)

    # home fatigue
    exp_home -= 0.6 * ph
    exp_away += 0.4 * ph

    # away fatigue
    exp_away -= 0.6 * pa
    exp_home += 0.4 * pa

    return exp_home, exp_away


def _injury_penalty(inj_flags: Dict[str, Any] | None) -> float:
    if not inj_flags:
        return 0.0
    counts = inj_flags.get("counts") or {}
    out_cnt = int(counts.get("OUT") or 0)
    doubtful_cnt = int(counts.get("DOUBTFUL") or 0)
    questionable_cnt = int(counts.get("QUESTIONABLE") or 0)
    # Penalidad base por volumen (rol players)
    penalty = (0.5 * out_cnt) + (0.3 * doubtful_cnt) + (0.1 * questionable_cnt)
    return min(3.0, penalty)


def _key_player_impact(inj_list: List[Dict[str, Any]], team_name: str) -> float:
    if not inj_list or not team_name:
        return 0.0
    
    # Buscar lista de estrellas para este equipo usando mapeo flexible
    stars = KEY_PLAYERS.get(team_name)
    if not stars:
        # Intentar matcheo por substring o alias común
        for k, v in KEY_PLAYERS.items():
            if k in team_name or team_name in k: 
                stars = v
                break
    
    if not stars:
        return 0.0

    impact = 0.0
    for inj in inj_list:
        player_name = inj.get("player")
        if not player_name:
            continue
            
        # Buscar player en stars (búsqueda laxa)
        tier = None
        for s_name, s_tier in stars.items():
            if s_name in player_name or player_name in s_name:
                tier = s_tier
                break
        
        if tier:
            status = (inj.get("status") or "").upper()
            weight = 0.0
            if status == "OUT":
                weight = 1.0
            elif status == "DOUBTFUL":
                weight = 0.6
            
            if weight > 0:
                imp_val = IMPACT_TIERS.get(tier, 1.0)
                impact += (imp_val * weight)
    
    return min(15.0, impact)  # Cap de impacto total


def _apply_injuries_bidirectional(
    exp_home: float,
    exp_away: float,
    home_inj_flags: Dict[str, Any] | None,
    away_inj_flags: Dict[str, Any] | None,
    home_inj_list: List[Dict[str, Any]] | None = None,
    away_inj_list: List[Dict[str, Any]] | None = None,
    home_name: str = "",
    away_name: str = "",
) -> Tuple[float, float]:
    """
    Simplificación útil:
      - Lesión reduce tu output (70%)
      - y empeora tu defensa (30%) => sube puntos del rival
    """
    ph_gen = _injury_penalty(home_inj_flags)
    pa_gen = _injury_penalty(away_inj_flags)

    ph_star = _key_player_impact(home_inj_list, home_name) if home_inj_list and home_name else 0.0
    pa_star = _key_player_impact(away_inj_list, away_name) if away_inj_list and away_name else 0.0

    ph = ph_gen + ph_star
    pa = pa_gen + pa_star

    exp_home -= 0.7 * ph
    exp_away += 0.3 * ph

    exp_away -= 0.7 * pa
    exp_home += 0.3 * pa

    return exp_home, exp_away


def _expected_points(
    home: Dict[str, Any],
    away: Dict[str, Any],
    home_rest: Dict[str, Any] | None = None,
    away_rest: Dict[str, Any] | None = None,
    home_inj_flags: Dict[str, Any] | None = None,
    away_inj_flags: Dict[str, Any] | None = None,
    # Nuevos parametros para key players
    home_inj_list: List[Dict[str, Any]] | None = None,
    away_inj_list: List[Dict[str, Any]] | None = None,
    home_name: str = "",
    away_name: str = "",
) -> Tuple[float, float, float, str, List[str]]:
    """
    Retorna:
      exp_home, exp_away, exp_pos,
      data_quality: "OK" | "LOW",
      missing: lista de campos faltantes (para flags)
    """
    missing: List[str] = []

    home_pf = _as_float(home.get("pf_pg"))
    home_pa = _as_float(home.get("pa_pg"))
    away_pf = _as_float(away.get("pf_pg"))
    away_pa = _as_float(away.get("pa_pg"))

    home_ortg = _as_float(home.get("ortg"))
    home_drtg = _as_float(home.get("drtg"))
    home_pace = _as_float(home.get("pace"))
    away_ortg = _as_float(away.get("ortg"))
    away_drtg = _as_float(away.get("drtg"))
    away_pace = _as_float(away.get("pace"))

    home_ortg_h = _as_float(home.get("home_ortg"))
    home_drtg_h = _as_float(home.get("home_drtg"))
    home_pace_h = _as_float(home.get("home_pace"))
    away_ortg_a = _as_float(away.get("away_ortg"))
    away_drtg_a = _as_float(away.get("away_drtg"))
    away_pace_a = _as_float(away.get("away_pace"))

    use_split_adv = all(
        v is not None
        for v in (home_ortg_h, home_drtg_h, home_pace_h, away_ortg_a, away_drtg_a, away_pace_a)
    )
    use_adv = all(v is not None for v in (home_ortg, home_drtg, home_pace, away_ortg, away_drtg, away_pace))

    if use_split_adv:
        # Ponderación agresiva de splits: 60% split, 40% overall para suavizar
        exp_pos_split = (home_pace_h + away_pace_a) / 2.0
        exp_pos_ovr = (home_pace + away_pace) / 2.0
        exp_pos = 0.7 * exp_pos_split + 0.3 * exp_pos_ovr
        # Pace-matchup: cuando ambos equipos son de alto ritmo, el partido genera
        # más posesiones de lo que indica el promedio simple.
        exp_pos += max(0.0, exp_pos - PACE_BASELINE) * PACE_INTERACTION
        
        # Ratings blended
        eff_ortg_h = 0.7 * home_ortg_h + 0.3 * home_ortg
        eff_drtg_a = 0.7 * away_drtg_a + 0.3 * away_drtg
        
        eff_ortg_a = 0.7 * away_ortg_a + 0.3 * away_ortg
        eff_drtg_h = 0.7 * home_drtg_h + 0.3 * home_drtg
        
        exp_home = ((eff_ortg_h + eff_drtg_a) / 2.0) * exp_pos / 100.0
        exp_away = ((eff_ortg_a + eff_drtg_h) / 2.0) * exp_pos / 100.0
        
    elif use_adv:
        exp_pos = (home_pace + away_pace) / 2.0
        exp_pos += max(0.0, exp_pos - PACE_BASELINE) * PACE_INTERACTION
        exp_home = ((home_ortg + away_drtg) / 2.0) * exp_pos / 100.0
        exp_away = ((away_ortg + home_drtg) / 2.0) * exp_pos / 100.0
    else:
        if home_pf is None:
            missing.append("HOME_PF")
            home_pf = 110.0
        if home_pa is None:
            missing.append("HOME_PA")
            home_pa = 110.0
        if away_pf is None:
            missing.append("AWAY_PF")
            away_pf = 110.0
        if away_pa is None:
            missing.append("AWAY_PA")
            away_pa = 110.0

        for key, val in (
            ("HOME_ORTG", home_ortg),
            ("HOME_DRTG", home_drtg),
            ("HOME_PACE", home_pace),
            ("AWAY_ORTG", away_ortg),
            ("AWAY_DRTG", away_drtg),
            ("AWAY_PACE", away_pace),
        ):
            if val is None:
                missing.append(key)

        exp_home = (home_pf + away_pa) / 2.0
        exp_away = (away_pf + home_pa) / 2.0
        exp_pos = (home_pace or away_pace or PACE_BASELINE)

    # ===== last10 blend (si está completo) =====
    last10_ortg = _as_float(home.get("last10_ortg"))
    last10_drtg = _as_float(home.get("last10_drtg"))
    last10_pace = _as_float(home.get("last10_pace"))
    last10_ortg_a = _as_float(away.get("last10_ortg"))
    last10_drtg_a = _as_float(away.get("last10_drtg"))
    last10_pace_a = _as_float(away.get("last10_pace"))
    if all(
        v is not None
        for v in (last10_ortg, last10_drtg, last10_pace, last10_ortg_a, last10_drtg_a, last10_pace_a)
    ):
        exp_pos_l10 = (last10_pace + last10_pace_a) / 2.0
        exp_pos_l10 += max(0.0, exp_pos_l10 - PACE_BASELINE) * PACE_INTERACTION
        exp_home_l10 = ((last10_ortg + last10_drtg_a) / 2.0) * exp_pos_l10 / 100.0
        exp_away_l10 = ((last10_ortg_a + last10_drtg) / 2.0) * exp_pos_l10 / 100.0
        exp_home = (1.0 - RECENT_WEIGHT) * exp_home + RECENT_WEIGHT * exp_home_l10
        exp_away = (1.0 - RECENT_WEIGHT) * exp_away + RECENT_WEIGHT * exp_away_l10
        exp_pos = (1.0 - RECENT_WEIGHT) * exp_pos + RECENT_WEIGHT * exp_pos_l10

    # ===== rest + injuries (bidirectional) =====
    exp_home, exp_away = _apply_rest_bidirectional(exp_home, exp_away, home_rest, away_rest)
    exp_home, exp_away = _apply_injuries_bidirectional(
        exp_home, exp_away, home_inj_flags, away_inj_flags,
        home_inj_list, away_inj_list, home_name, away_name
    )

    # clamp razonable
    exp_home = max(70.0, exp_home)
    exp_away = max(70.0, exp_away)

    data_quality = "LOW" if missing else "OK"
    return exp_home, exp_away, exp_pos, data_quality, missing


def _injuries_suspect(context: Dict[str, Any]) -> bool:
    """
    Arreglo tipster:
    - NO marcar suspect solo porque counts=0.
    - "SUSPECT" únicamente si tu pipeline declara que *intentó parsear* injuries
      pero no entregó estructura mínima.
    Reglas:
      1) Si injuries_unknown=True => no suspect.
      2) Si existe counts dict en alguno de los flags => no suspect (aunque sean ceros).
      3) Si existen listas home_injuries/away_injuries => no suspect.
      4) Si context.injuries_parsed=True y no hay counts/listas => suspect.
      5) Si no hay señal de parseo => no suspect (no penalizamos).
    """
    if bool(context.get("injuries_unknown")):
        return False

    home_inj = context.get("home_injuries") or []
    away_inj = context.get("away_injuries") or []
    if home_inj or away_inj:
        return False

    hf = context.get("home_injury_flags") or {}
    af = context.get("away_injury_flags") or {}

    def _has_counts(flags: Dict[str, Any]) -> bool:
        return isinstance((flags or {}).get("counts"), dict)

    if _has_counts(hf) or _has_counts(af):
        return False

    return bool(context.get("injuries_parsed"))


def _p_implied_pair_if_same_book(
    odds_a: float | None,
    odds_b: float | None,
    book_a: Any | None,
    book_b: Any | None,
) -> Tuple[float | None, float | None]:
    """
    Solo quitamos vig si ambos lados vienen del MISMO book; si no, devuelve (None, None)
    para que caiga al fallback 1/odds individual.
    """
    if not book_a or not book_b:
        return None, None
    if str(book_a) != str(book_b):
        return None, None
    return _p_implied_pair(odds_a, odds_b)


def _market_thresholds(market: str, line: float | None) -> Tuple[float, float]:
    """Devuelve (p_min, ev_min) por mercado y condiciones."""
    if market == "ML":
        return P_MIN_ML, EV_MIN_ML
    if market == "SPREAD":
        return P_MIN_SPREAD, EV_MIN_SPREAD
    if market == "TOTAL":
        if isinstance(line, (int, float)) and line >= TOTAL_HIGH_LINE:
            return max(P_MIN_TOTAL, P_MIN_TOTAL_HIGH), EV_MIN_TOTAL
        return P_MIN_TOTAL, EV_MIN_TOTAL
    return P_MIN, EV_MIN


def _edge_min_for_market(market: str) -> float:
    if market == "ML":
        return max(EDGE_MIN, EDGE_MIN_ML)
    if market == "SPREAD":
        return max(EDGE_MIN, EDGE_MIN_SPREAD)
    return EDGE_MIN


def _inj_severity(flags: Dict[str, Any] | None) -> int:
    counts = (flags or {}).get("counts") or {}
    out_cnt = int(counts.get("OUT") or 0)
    doubtful_cnt = int(counts.get("DOUBTFUL") or 0)
    return out_cnt + doubtful_cnt


def _sigma_with_sample(base_sigma: float, games_home: Any, games_away: Any) -> float:
    """
    Inflamos sigma cuando hay poca muestra => reduce sobreconfianza del p̂.
    Usamos el mínimo entre home/away games como referencia conservadora.
    """
    try:
        g1 = int(games_home) if games_home is not None else 0
        g2 = int(games_away) if games_away is not None else 0
    except Exception:
        g1, g2 = 0, 0

    g = max(1, min(g1, g2))
    if g >= SIGMA_SAMPLE_REF:
        return base_sigma

    factor = math.sqrt(SIGMA_SAMPLE_REF / g)
    factor = min(SIGMA_SAMPLE_CAP, max(1.0, factor))
    return base_sigma * factor


def _annotate_books_counts(candidates: List[Dict[str, Any]]) -> None:
    """
    Agrega c['books_count'] por bucket (market+pick+line si aplica).
    """
    book_sets: Dict[Tuple[Any, Any, Any], set] = {}
    for c in candidates:
        mkt = c.get("market")
        pick = c.get("pick")
        line = c.get("line") if mkt in ("TOTAL", "SPREAD") else None
        key = (mkt, pick, line)
        bk = c.get("book")
        if not bk:
            continue
        book_sets.setdefault(key, set()).add(str(bk))

    for c in candidates:
        mkt = c.get("market")
        pick = c.get("pick")
        line = c.get("line") if mkt in ("TOTAL", "SPREAD") else None
        key = (mkt, pick, line)
        c["books_count"] = len(book_sets.get(key, set()))


def _candidate_status(c: Dict[str, Any], flags: List[str]) -> str:
    """
    Decide BET / BET_DEGRADED / LEAN / NO_BET para un candidato.

    Cambio clave:
      - INJ_UNKNOWN ya NO fuerza LEAN.
      - Si INJ_UNKNOWN: permitimos BET_DEGRADED SOLO en TOTAL/SPREAD con thresholds más duros.
    """
    market = c.get("market")
    pick = c.get("pick")
    p_hat = c.get("p_hat")
    odds = c.get("odds")
    ev = c.get("ev")
    edge = c.get("edge")
    line = c.get("line")
    books_count = int(c.get("books_count") or 0)

    if odds is None or odds < ODDS_MIN:
        return "NO_BET"
    if not isinstance(p_hat, (int, float)):
        return "NO_BET"

    # mínimos de consenso
    if books_count < MIN_BOOKS_LEAN:
        return "NO_BET"

    inj_unknown = "INJ_UNKNOWN" in flags
    inj_suspect = "INJ_SUSPECT" in flags

    # thresholds base por mercado
    p_min, ev_min = _market_thresholds(market, line)

    if market == "TOTAL" and str(pick).upper() == "OVER":
        p_min = max(p_min, P_MIN_TOTAL_OVER)
    if market == "TOTAL" and str(pick).upper() == "UNDER":
        p_min = max(p_min, P_MIN_TOTAL_UNDER)

    # guard rails específicos
    if market == "SPREAD":
        if "INJ_SEV4PLUS" in flags:
            return "NO_BET"
        if "INJ_SEV_WARN" in flags:
            p_min += INJ_SPREAD_SEV1_P_ADD

        if isinstance(line, (int, float)) and abs(line) >= BIG_SPREAD_ABS:
            return "NO_BET"

        # Nota: ML_MARKET_DIVERGENCE NO se propaga al SPREAD.
        # El SPREAD evalúa márgenes directamente y tiene sus propios gates de edge.

    # === NUEVO: TOTALES también rechazan SEV4PLUS ===
    if market == "TOTAL":
        if "INJ_SEV4PLUS" in flags:
            return "NO_BET"

    edge_min = _edge_min_for_market(market)
    if market == "TOTAL" and str(pick).upper() == "OVER":
        edge_min = max(edge_min, EDGE_MIN_TOTAL_OVER)
    if market == "TOTAL" and str(pick).upper() == "UNDER":
        edge_min = max(edge_min, EDGE_MIN_TOTAL_UNDER)
    if isinstance(edge, (int, float)) and edge < edge_min:
        return "NO_BET"

    # ===== MODO DEGRADADO (INJ_UNKNOWN) =====
    if inj_unknown:
        # no ML en degradado (alta varianza sin info)
        if market == "ML":
            return "NO_BET"
        if market not in ("TOTAL", "SPREAD"):
            return "NO_BET"

        # thresholds duros (los que validaste con SQL)
        if odds >= DEG_INJ_ODDS_MIN and p_hat >= DEG_INJ_P_MIN and isinstance(ev, (int, float)) and ev >= DEG_INJ_EV_MIN:
            # BET exige consenso mínimo
            if books_count < MIN_BOOKS_BET:
                return "LEAN"
            return "BET_DEGRADED"
        return "LEAN"

    # ===== NORMAL (cuando NO es INJ_UNKNOWN) =====

    # Fix 1: INJ_SEV4PLUS en ML => LEAN.
    # Un jugador clave (S+/S/A/B) ausente introduce demasiada incertidumbre en el
    # expected_margin; el modelo de lambda no puede valorarla bien => no apostar ML.
    if market == "ML" and "INJ_SEV4PLUS" in flags:
        if "INJ_DEGRADED" not in flags:
            flags.append("INJ_DEGRADED")
        return "LEAN"

    # Pinnacle divergence gate: edge muy alto en ML => modelo probablemente equivocado
    if market == "ML" and isinstance(edge, (int, float)) and edge > EDGE_ML_SUSPICION:
        flags.append("ML_MARKET_DIVERGENCE")
        return "LEAN"

    # ML_MARKET_DIVERGENCE no se propaga al TOTAL.
    # El TOTAL tiene su propio gate (EDGE_TOTAL_SUSPICION) más abajo.

    # Fix 2b: TOTAL con edge extremo vs Pinnacle (independiente del ML).
    # Edge > EDGE_TOTAL_SUSPICION indica que el modelo no está capturando info del mercado
    # (p.ej. lesiones severas que Pinnacle ya descontó pero los lambdas no).
    if market == "TOTAL" and isinstance(edge, (int, float)) and edge > EDGE_TOTAL_SUSPICION:
        if "TOTAL_MARKET_DIVERGENCE" not in flags:
            flags.append("TOTAL_MARKET_DIVERGENCE")
        return "LEAN"

    if p_hat < p_min:
        return "NO_BET"
    if isinstance(ev, (int, float)) and ev < ev_min:
        return "NO_BET"

    # pasa filtros => BET, pero degradamos a LEAN si hay riesgo
    risk = False

    # consenso: BET solo si hay >= MIN_BOOKS_BET
    if books_count < MIN_BOOKS_BET:
        risk = True

    if "DATA_LOW" in flags or inj_suspect:
        risk = True

    if market == "TOTAL" and TOTAL_HIGH_AUTO_LEAN:
        if isinstance(line, (int, float)) and line >= TOTAL_HIGH_LINE:
            risk = True

    return "LEAN" if risk else "BET"


def _pick_best_v2(candidates: List[Dict[str, Any]], flags: List[str]) -> Tuple[Dict[str, Any], str]:
    """
    Selecciona el mejor candidato considerando:
      - status BET/BET_DEGRADED/LEAN/NO_BET por mercado y flags
      - desempate por p_hat y EV
    """
    if not candidates:
        return {}, "NO_PICK"

    scored: List[Tuple[str, Dict[str, Any]]] = []
    for c in candidates:
        status = _candidate_status(c, flags)
        scored.append((status, c))

    priority = {"BET": 3, "BET_DEGRADED": 2, "LEAN": 1, "NO_BET": 0}

    best_status, best = max(
        scored,
        key=lambda t: (
            priority.get(t[0], 0),
            t[1].get("p_hat") or 0.0,
            t[1].get("ev") if isinstance(t[1].get("ev"), (int, float)) else -9999.0,
        ),
    )

    if best_status in ("BET", "BET_DEGRADED"):
        return best, best_status
    if best_status == "LEAN":
        return best, "LEAN"
    return best, "NO_BET"


def main() -> None:
    ctx = _read_json(CONTEXT_PATH) or []
    if isinstance(ctx, dict):
        ctx = ctx.get("rows") or []
    if not isinstance(ctx, list):
        ctx = []
    if not ctx:
        Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
        Path(OUT_TSV).parent.mkdir(parents=True, exist_ok=True)
        Path(OUT_PATH).write_text(json.dumps([], ensure_ascii=False, indent=2), encoding="utf-8")
        Path(OUT_TSV).write_text(TSV_HEADER + "\n", encoding="utf-8")
        print(f"OK -> {OUT_PATH} (items=0)")
        print(f"OK -> {OUT_TSV}")
        return

    run_id = uuid.uuid4().hex
    created_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    out: List[Dict[str, Any]] = []
    tsv_lines = [TSV_HEADER]

    # Dedup: por match_id (preferible) + market/pick/line
    seen: set[tuple] = set()

    for item in ctx:
        match = item.get("match") or {}
        odds = item.get("odds") or {}
        context = item.get("context") or {}

        home = match.get("home") or ""
        away = match.get("away") or ""
        kickoff = _sanitize_utc_kickoff(match.get("utc_kickoff") or "")

        # PATCH: Si viene '00:00:00', intentamos rescatar la hora del status (ej: 7:30 pm ET)
        if "00:00:00" in kickoff:
            st = match.get("status") or ""
            fixed = _parse_et_status(DAY, st)
            if fixed:
                kickoff = fixed

        # id preferido: game_id / match_id / id; fallback por llave estable
        match_id = match.get("game_id") or match.get("match_id") or match.get("id")
        if not match_id:
            match_id = f"{kickoff}|{home}|{away}"

        home_strength = context.get("home_strength") or {}
        away_strength = context.get("away_strength") or {}
        home_rest = context.get("home_rest") or {}
        away_rest = context.get("away_rest") or {}
        home_inj_flags = context.get("home_injury_flags") or {}
        away_inj_flags = context.get("away_injury_flags") or {}

        # Listas de lesionados (para key players)
        home_inj_list = context.get("home_injuries") or []
        away_inj_list = context.get("away_injuries") or []

        # games (para sigma inflation)
        hh_games = home_strength.get("games")
        aa_games = away_strength.get("games")

        exp_home, exp_away, exp_pos, data_quality, missing_stats = _expected_points(
            home_strength,
            away_strength,
            home_rest,
            away_rest,
            home_inj_flags,
            away_inj_flags,
            home_inj_list,
            away_inj_list,
            home,
            away,
        )

        exp_total = exp_home + exp_away
        exp_margin = exp_home - exp_away

        pace_factor = (exp_pos / PACE_BASELINE) if exp_pos else 1.0
        sigma_margin = min(max(MARGIN_SIGMA * pace_factor, SIGMA_MIN_MARGIN), SIGMA_MAX_MARGIN)
        sigma_total = min(max(TOTAL_SIGMA * pace_factor, SIGMA_MIN_TOTAL), SIGMA_MAX_TOTAL)

        # sigma inflation por sample size (reduce sobreconfianza)
        sigma_margin = _sigma_with_sample(sigma_margin, hh_games, aa_games)
        sigma_total = _sigma_with_sample(sigma_total, hh_games, aa_games)

        # Pace-high sigma: partidos de alto ritmo tienen distribución de totales
        # más ancha → se reduce overconfidence en picks OVER/UNDER.
        if exp_pos and exp_pos > PACE_HIGH_THRESHOLD:
            sigma_total = min(SIGMA_MAX_TOTAL, sigma_total * PACE_HIGH_SIGMA_MULT)

        # Base probs "tipo fútbol" solo para no romper esquema (NBA no tiene draw)
        p_home_base = _norm_cdf(exp_margin / sigma_margin) if sigma_margin else None
        p_away_base = (1.0 - p_home_base) if isinstance(p_home_base, (int, float)) else None
        p_draw_base = 0.0

        candidates: List[Dict[str, Any]] = []

        # ======================
        # ML (HOME/AWAY)
        # ======================
        ml = (odds.get("best") or {}).get("ml") or {}
        odds_home = float(ml["HOME"]["odds"]) if ml.get("HOME") else None
        odds_away = float(ml["AWAY"]["odds"]) if ml.get("AWAY") else None
        book_home = ml["HOME"].get("book") if ml.get("HOME") else None
        book_away = ml["AWAY"].get("book") if ml.get("AWAY") else None

        p_imp_home, p_imp_away = _p_implied_pair_if_same_book(odds_home, odds_away, book_home, book_away)

        if p_imp_home is None and odds_home:
            p_imp_home = _p_implied_decimal(odds_home)
        if p_imp_away is None and odds_away:
            p_imp_away = _p_implied_decimal(odds_away)

        if ml.get("HOME") and odds_home:
            p_hat = _norm_cdf(exp_margin / sigma_margin)
            candidates.append(
                {
                    "market": "ML",
                    "pick": "HOME",
                    "line": None,
                    "p_hat": p_hat,
                    "odds": odds_home,
                    "book": book_home,
                    "p_implied": p_imp_home,
                    "edge": (p_hat - p_imp_home) if p_imp_home is not None else None,
                    "ev": _ev_decimal(p_hat, odds_home),
                }
            )

        if ml.get("AWAY") and odds_away:
            p_hat = _norm_cdf((-exp_margin) / sigma_margin)
            candidates.append(
                {
                    "market": "ML",
                    "pick": "AWAY",
                    "line": None,
                    "p_hat": p_hat,
                    "odds": odds_away,
                    "book": book_away,
                    "p_implied": p_imp_away,
                    "edge": (p_hat - p_imp_away) if p_imp_away is not None else None,
                    "ev": _ev_decimal(p_hat, odds_away),
                }
            )

        # ======================
        # TOTAL (OVER/UNDER)
        # ======================
        totals = (odds.get("best") or {}).get("totals_by_line") or {}
        for line_str, sides in totals.items():
            line = _as_float(line_str)
            if line is None:
                continue

            over_side = (sides or {}).get("over")
            under_side = (sides or {}).get("under")

            odds_over = float(over_side["odds"]) if over_side and over_side.get("odds") is not None else None
            odds_under = float(under_side["odds"]) if under_side and under_side.get("odds") is not None else None
            book_over = over_side.get("book") if over_side else None
            book_under = under_side.get("book") if under_side else None

            p_imp_over, p_imp_under = _p_implied_pair_if_same_book(odds_over, odds_under, book_over, book_under)
            if p_imp_over is None and odds_over:
                p_imp_over = _p_implied_decimal(odds_over)
            if p_imp_under is None and odds_under:
                p_imp_under = _p_implied_decimal(odds_under)
            sigma_used = sigma_total * (TOTAL_HIGH_SIGMA_MULT if line >= TOTAL_HIGH_LINE else 1.0)

            if over_side and odds_over:
                z = (exp_total - line) / sigma_used
                p_hat = _calibrate_total_p(z=z, odds=odds_over, line=float(line), pick="OVER")
                candidates.append(
                    {
                        "market": "TOTAL",
                        "pick": "OVER",
                        "line": float(line),
                        "p_hat": p_hat,
                        "odds": odds_over,
                        "book": book_over,
                        "p_implied": p_imp_over,
                        "edge": (p_hat - p_imp_over) if p_imp_over is not None else None,
                        "ev": _ev_decimal(p_hat, odds_over),
                    }
                )

            if under_side and odds_under:
                z = (line - exp_total) / sigma_used
                p_hat = _calibrate_total_p(z=z, odds=odds_under, line=float(line), pick="UNDER")
                candidates.append(
                    {
                        "market": "TOTAL",
                        "pick": "UNDER",
                        "line": float(line),
                        "p_hat": p_hat,
                        "odds": odds_under,
                        "book": book_under,
                        "p_implied": p_imp_under,
                        "edge": (p_hat - p_imp_under) if p_imp_under is not None else None,
                        "ev": _ev_decimal(p_hat, odds_under),
                    }
                )



        # ======================
        # SPREAD
        # ======================
        spreads = (odds.get("best") or {}).get("spreads_by_line") or {}
        for line_str, sides in spreads.items():
            key_line = _as_float(line_str)
            abs_line = abs(key_line) if isinstance(key_line, (int, float)) else None

            home_side = (sides or {}).get("home")
            away_side = (sides or {}).get("away")

            odds_home_sp = float(home_side["odds"]) if home_side and home_side.get("odds") is not None else None
            odds_away_sp = float(away_side["odds"]) if away_side and away_side.get("odds") is not None else None
            book_home_sp = home_side.get("book") if home_side else None
            book_away_sp = away_side.get("book") if away_side else None

            p_imp_home_sp, p_imp_away_sp = _p_implied_pair_if_same_book(
                odds_home_sp, odds_away_sp, book_home_sp, book_away_sp
            )
            if p_imp_home_sp is None and odds_home_sp:
                p_imp_home_sp = _p_implied_decimal(odds_home_sp)
            if p_imp_away_sp is None and odds_away_sp:
                p_imp_away_sp = _p_implied_decimal(odds_away_sp)

            if home_side and odds_home_sp:
                line_val = _as_float(home_side.get("line"))
                if line_val is None and abs_line is not None:
                    line_val = -abs_line  # HOME favorito => negativo
                if line_val is not None:
                    p_hat = _norm_cdf((exp_margin + float(line_val)) / sigma_margin)
                    candidates.append(
                        {
                            "market": "SPREAD",
                            "pick": "HOME",
                            "line": float(line_val),
                            "p_hat": p_hat,
                            "odds": odds_home_sp,
                            "book": book_home_sp,
                            "p_implied": p_imp_home_sp,
                            "edge": (p_hat - p_imp_home_sp) if p_imp_home_sp is not None else None,
                            "ev": _ev_decimal(p_hat, odds_home_sp),
                        }
                    )

            if away_side and odds_away_sp:
                line_val = _as_float(away_side.get("line"))
                if line_val is None and abs_line is not None:
                    line_val = abs_line  # AWAY +abs
                if line_val is not None:
                    p_hat = _norm_cdf((float(line_val) - exp_margin) / sigma_margin)
                    candidates.append(
                        {
                            "market": "SPREAD",
                            "pick": "AWAY",
                            "line": float(line_val),
                            "p_hat": p_hat,
                            "odds": odds_away_sp,
                            "book": book_away_sp,
                            "p_implied": p_imp_away_sp,
                            "edge": (p_hat - p_imp_away_sp) if p_imp_away_sp is not None else None,
                            "ev": _ev_decimal(p_hat, odds_away_sp),
                        }
                    )

        # books_count por candidato (para consenso BET/LEAN)
        _annotate_books_counts(candidates)

        # ======================
        # Injuries gates + flags (antes de elegir pick)
        # ======================
        injuries_unknown = bool(context.get("injuries_unknown"))
        injuries_suspect = _injuries_suspect(context)

        home_flags = context.get("home_injury_flags") or {}
        away_flags = context.get("away_injury_flags") or {}
        has_out = bool(home_flags.get("has_out_or_doubtful") or away_flags.get("has_out_or_doubtful"))

        base_flags: List[str] = []
        if data_quality == "LOW":
            base_flags.append("DATA_LOW")
            if missing_stats:
                base_flags.append("MISSING_" + "|".join(missing_stats))

        if injuries_unknown:
            base_flags.append("INJ_UNKNOWN")
        elif injuries_suspect:
            base_flags.append("INJ_SUSPECT")

        if has_out:
            base_flags.append("HAS_OUT")

        # severidad (OUT+DOUBTFUL total entre ambos equipos)
        sev = _inj_severity(home_flags) + _inj_severity(away_flags)
        if sev >= 4:
            base_flags.append("INJ_SEV4PLUS")
        elif sev >= 1:
            base_flags.append("INJ_SEV_WARN")

        def _empty_pick(market: str) -> Dict[str, Any]:
            return {
                "market": market,
                "pick": "NO_PICK",
                "line": None,
                "p_hat": None,
                "odds": None,
                "book": None,
                "p_implied": None,
                "edge": None,
                "ev": None,
                "books_count": 0,
            }

        def _adjust_decision(decision: str, pick: Dict[str, Any], dq: str, inj_unknown: bool, inj_suspect: bool) -> str:
            """
            Ajustes post-selección:
              - DATA_LOW: si BET => LEAN (y si EV<0 => NO_PICK)
              - INJ_UNKNOWN: si BET => BET_DEGRADED (ya NO lo bajamos a LEAN)
              - INJ_SUSPECT: si BET => LEAN
            """
            best_ev = pick.get("ev") if isinstance(pick, dict) else None

            if dq == "LOW":
                if isinstance(best_ev, (int, float)) and best_ev < 0:
                    return "NO_PICK"
                if decision == "BET":
                    decision = "LEAN"

            if inj_unknown and decision == "BET":
                decision = "BET_DEGRADED"

            if inj_suspect and decision == "BET":
                decision = "LEAN"

            return decision

        def _stake_for_pick(pick: Dict[str, Any], decision: str, flags_list: List[str]) -> float:
            if decision not in ("BET", "LEAN", "BET_DEGRADED"):
                return 0.0

            # Kelly
            stake = _stake_units(pick.get("p_hat"), pick.get("odds"))
            
            # Si el stake calculado es minúsculo, lo matamos
            if stake < 0.01:
                return 0.0

            mkt = pick.get("market")

            # Hard caps por mercado (seguridad adicional)
            if mkt == "TOTAL":
                stake = min(stake, STAKE_CAP_TOTAL)
            elif mkt == "SPREAD":
                stake = min(stake, STAKE_CAP_SPREAD)
                if "INJ_SEV_WARN" in flags_list:
                    stake = min(stake, STAKE_CAP_SPREAD_INJ_SEV1)

            if decision == "LEAN":
                # LEAN reduce stake a la mitad o cap
                stake = min(stake, STAKE_CAP_LEAN)
                stake = stake * 0.5 

            if decision == "BET_DEGRADED":
                stake = min(stake, DEG_INJ_STAKE_CAP)

            return round(stake, 3)

        def _is_primary(pick: Dict[str, Any]) -> bool:
            try:
                return pick.get("odds") is not None and pick.get("p_hat") is not None and float(pick["odds"]) >= 1.6
            except Exception:
                return False

        def _bet_level(pick: Dict[str, Any], decision: str) -> int:
            """
            bet=1 SOLO para BET. BET_DEGRADED/LEAN/NO_BET = 0.
            El usuario solo apuesta decision=BET; BET_DEGRADED es informativo.
            """
            d = str(decision or "").upper()
            if d == "BET":
                return 1
            return 0

        rows: List[Tuple[Dict[str, Any], str, List[str]]] = []
        best_picks: Dict[str, Any] = {}

        for market in ("ML", "SPREAD", "TOTAL"):
            market_candidates = [c for c in candidates if c.get("market") == market]
            pick, decision = _pick_best_v2(market_candidates, base_flags)
            if not pick:
                pick = _empty_pick(market)

            decision = _adjust_decision(decision, pick, data_quality, injuries_unknown, injuries_suspect)
            pick = dict(pick)
            pick["stake_u"] = _stake_for_pick(pick, decision, base_flags)

            row_flags = list(base_flags)
            if decision == "BET_DEGRADED":
                row_flags.append("DEGRADED_INJ_UNKNOWN")
            if _is_primary(pick):
                row_flags.append("PRIMARY")

            rows.append((pick, decision, row_flags))
            best_picks[market] = {
                "decision": decision,
                "pick": pick,
                "flags": row_flags,
                "books_count_pick": int(pick.get("books_count") or 0),
            }

        # ===== Fix 3: concentración — máximo 1 BET por partido =====
        # Si hay >1 BET en el mismo partido, solo el de mayor EDGE mantiene BET;
        # los demás bajan a LEAN + CONC_DOWNGRADE.
        # Criterio: edge (p_hat - p_implied) en lugar de EV — mide la ventaja pura
        # sobre Pinnacle sin inflar por las odds. Kelly óptimo ∝ edge/(odds-1).
        # Razón: BETs correlacionados en el mismo partido = N× exposición al mismo evento.
        bet_indices = [i for i, (_, dec, _) in enumerate(rows) if dec == "BET"]
        if len(bet_indices) > 1:
            best_i = max(bet_indices, key=lambda i: float(rows[i][0].get("edge") or -9999.0))
            new_rows = []
            for i, (rp, rd, rf) in enumerate(rows):
                if i in bet_indices and i != best_i:
                    rp = dict(rp)
                    rf = list(rf) + ["CONC_DOWNGRADE"]
                    rd = "LEAN"
                    rp["stake_u"] = _stake_for_pick(rp, "LEAN", rf)
                    mkt = rp.get("market")
                    if mkt and mkt in best_picks:
                        best_picks[mkt] = {
                            "decision": rd,
                            "pick": rp,
                            "flags": rf,
                            "books_count_pick": int(rp.get("books_count") or 0),
                        }
                new_rows.append((rp, rd, rf))
            rows = new_rows

        # Best overall (compatibilidad)
        overall_pick, overall_decision = _pick_best_v2(candidates, base_flags)
        if not overall_pick:
            overall_pick = _empty_pick("NO_PICK")

        overall_decision = _adjust_decision(overall_decision, overall_pick, data_quality, injuries_unknown, injuries_suspect)
        overall_pick = dict(overall_pick)
        overall_pick["stake_u"] = _stake_for_pick(overall_pick, overall_decision, base_flags)
        overall_flags = list(base_flags)
        if overall_decision == "BET_DEGRADED":
            overall_flags.append("DEGRADED_INJ_UNKNOWN")
        if _is_primary(overall_pick):
            overall_flags.append("PRIMARY")
        overall_books_count = int(overall_pick.get("books_count") or 0)

        match_label = f"{home} vs {away}"
        for row_pick, decision, row_flags in rows:
            row_key = (
                str(match_id),
                row_pick.get("market"),
                row_pick.get("pick"),
                row_pick.get("line"),
            )
            if row_key in seen:
                continue
            seen.add(row_key)

            books_count = int(row_pick.get("books_count") or 0)
            p_hat = row_pick.get("p_hat")
            odds_val = row_pick.get("odds")
            ev_val = row_pick.get("ev")
            p_implied = row_pick.get("p_implied")
            edge = row_pick.get("edge")
            stake_u = row_pick.get("stake_u", 0.0)

            bet_level = _bet_level(row_pick, decision)
            row_pick["bet"] = bet_level

            # Escribe TSV alineado EXACTO a TSV_COLS
            tsv_lines.append(
                "\t".join(
                    [
                        run_id,
                        created_at_utc,
                        kickoff,
                        home,
                        away,
                        match_label,
                        str(match_id),
                        row_pick.get("market", ""),
                        str(row_pick.get("pick", "")),
                        "" if row_pick.get("line") is None else str(row_pick.get("line")),
                        "" if odds_val is None else f"{float(odds_val):.3f}",
                        "" if row_pick.get("book") is None else str(row_pick.get("book")),
                        str(books_count),

                        # lambda_* (placeholders / compat)
                        f"{exp_home:.4f}",
                        f"{exp_away:.4f}",

                        # p_home/p_draw/p_away (base probs derivadas de margin)
                        f"{p_home_base:.4f}" if isinstance(p_home_base, (int, float)) else "",
                        f"{p_draw_base:.4f}",
                        f"{p_away_base:.4f}" if isinstance(p_away_base, (int, float)) else "",

                        # exp_*
                        f"{exp_home:.4f}",
                        f"{exp_away:.4f}",
                        f"{exp_total:.4f}",
                        f"{exp_margin:.4f}",

                        f"{stake_u:.2f}",
                        f"{p_hat:.4f}" if isinstance(p_hat, (int, float)) else "",
                        f"{p_implied:.4f}" if isinstance(p_implied, (int, float)) else "",
                        f"{edge:.4f}" if isinstance(edge, (int, float)) else "",
                        "" if ev_val is None else f"{float(ev_val):.4f}",
                        "" if hh_games is None else str(hh_games),
                        "" if aa_games is None else str(aa_games),
                        decision,
                        ";".join(row_flags),
                        str(bet_level),
                        "",
                    ]
                )
            )

        out.append(
            {
                "source": item,
                "match": match,
                "meta": {"match_id": str(match_id), "kickoff_utc_sanitized": kickoff},
                "model": {
                    "expected_home": exp_home,
                    "expected_away": exp_away,
                    "expected_total": exp_total,
                    "expected_margin": exp_margin,
                    "data_quality": data_quality,
                    "missing_stats": missing_stats,
                    "base_probs": {"p_home": p_home_base, "p_draw": p_draw_base, "p_away": p_away_base},
                    "params": {
                        "margin_sigma": sigma_margin,
                        "total_sigma": sigma_total,
                        "pace_used": exp_pos,
                        "sigma_sample_ref": SIGMA_SAMPLE_REF,
                        "sigma_sample_cap": SIGMA_SAMPLE_CAP,
                    },
                    "degraded_mode": {
                        "enabled_when": "INJ_UNKNOWN",
                        "odds_min": DEG_INJ_ODDS_MIN,
                        "p_min": DEG_INJ_P_MIN,
                        "ev_min": DEG_INJ_EV_MIN,
                        "stake_cap": DEG_INJ_STAKE_CAP,
                        "markets_allowed": ["TOTAL", "SPREAD"],
                        "ml_blocked": True,
                    },
                },
                "best_pick": {
                    "decision": overall_decision,
                    "pick": overall_pick,
                    "flags": overall_flags,
                    "books_count_pick": overall_books_count,
                },
                "best_picks": best_picks,
                "candidates": candidates,
            }
        )

    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_TSV).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_PATH).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(OUT_TSV).write_text("\n".join(tsv_lines), encoding="utf-8")
    print(f"OK -> {OUT_PATH} (items={len(out)})")
    print(f"OK -> {OUT_TSV}")

    if os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL"):
        tsv_text = Path(OUT_TSV).read_text(encoding="utf-8")
        _db_put_picks(LEAGUE, DAY, tsv_text)
        _db_put_picks_rows(LEAGUE, DAY, tsv_text)


if __name__ == "__main__":
    main()
