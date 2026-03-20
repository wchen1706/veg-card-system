import streamlit as st
import pandas as pd
from sqlalchemy import text, create_engine
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

# 1. 建立数据库云端引擎
@st.cache_resource
def init_engine():
    db_url = st.secrets["connections"]["supabase"]["url"]
    return create_engine(db_url)

engine = init_engine()

# 2. 专属查询函数
def run_query(sql_str, params=None):
    with engine.connect() as connection:
        return pd.read_sql(text(sql_str), connection, params=params)

# 3. 专属单次修改函数
def execute_query(sql_str, params=None):
    with engine.begin() as connection:
        connection.execute(text(sql_str), params)

def compute_card_status(total_weight: float, remaining_weight: float) -> str:
    if remaining_weight < 0:
        return "已扣超（欠费）"
    if remaining_weight == 0:
        return "已用完"
    if remaining_weight == total_weight:
        return "未使用新卡"
    return "使用中"

def get_member_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    df = run_query(
        "SELECT * FROM members WHERE phone = :phone LIMIT 1",
        params={"phone": phone}
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()

def create_member(name: str, wechat_name: str, phone: str) -> int:
    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                INSERT INTO members (name, wechat_name, phone, created_at)
                VALUES (:name, :wechat_name, :phone, (NOW() AT TIME ZONE 'PRC'))
                RETURNING id
                """
            ),
            {"name": name, "wechat_name": wechat_name, "phone": phone},
        ).fetchone()
        return int(row[0])

def _cycle_to_deliveries(cycle_type: str) -> int:
    if cycle_type == "month":
        return 4
    if cycle_type == "quarter":
        return 12
    if cycle_type == "year":
        return 50
    raise ValueError("Invalid cycle_type")

def create_card_with_debt_fill(
    member_id: int, spec_kg: int, cycle_type: str, purchase_date, operator: str = "系统"
) -> int:
    total_deliveries = _cycle_to_deliveries(cycle_type)
    total_weight = float(spec_kg * total_deliveries)

    with engine.begin() as connection:
        new_card_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO cards (
                        member_id, spec_kg_per_delivery, cycle_type,
                        total_deliveries, total_weight, remaining_weight,
                        purchase_date, status
                    ) VALUES (
                        :member_id, :spec, :cycle_type,
                        :deliveries, :total_weight, :remaining_weight,
                        :purchase_date, :status
                    )
                    RETURNING id
                    """
                ),
                {
                    "member_id": member_id, "spec": spec_kg, "cycle_type": cycle_type,
                    "deliveries": total_deliveries, "total_weight": total_weight,
                    "remaining_weight": total_weight, "purchase_date": purchase_date,
                    "status": "未使用新卡"
                },
            ).fetchone()[0]
        )

        debt_cards = connection.execute(
            text(
                """
                SELECT id, total_weight, remaining_weight
                FROM cards
                WHERE member_id = :member_id
                  AND remaining_weight < 0
                  AND id != :new_card_id
                ORDER BY purchase_date ASC, id ASC
                """
            ),
            {"member_id": member_id, "new_card_id": new_card_id},
        ).fetchall()

        new_remaining = total_weight

        for old_id, old_total, old_remaining in debt_cards:
            if new_remaining <= 0:
                break

            old_remaining = float(old_remaining)
            debt = -old_remaining
            if debt <= 0:
                continue

            offset = min(debt, new_remaining)
            new_old_remaining = old_remaining + offset
            if new_old_remaining > 0:
                new_old_remaining = 0.0

            old_status = compute_card_status(float(old_total), float(new_old_remaining))
            connection.execute(
                text("UPDATE cards SET remaining_weight = :rw, status = :st WHERE id = :id"),
                {"rw": new_old_remaining, "st": old_status, "id": int(old_id)},
            )

            new_remaining -= offset

            connection.execute(
                text(
                    """
                    INSERT INTO records (
                        card_id, member_id, op_date, delivery_date,
                        weight, status, created_at, operator
                    ) VALUES (
                        :card_id, :member_id, (NOW() AT TIME ZONE 'PRC')::date, (NOW() AT TIME ZONE 'PRC')::date,
                        :weight, :status, (NOW() AT TIME ZONE 'PRC'), :operator
                    )
                    """
                ),
                {
                    "card_id": new_card_id, "member_id": member_id,
                    "weight": float(offset),
                    "status": f"新卡自动抵扣旧卡欠费 {float(offset):.2f} 斤（旧卡ID:{int(old_id)}）",
                    "operator": operator
                },
            )

        new_status = compute_card_status(total_weight, float(new_remaining))
        connection.execute(
            text("UPDATE cards SET remaining_weight = :rw, status = :st WHERE id = :id"),
            {"rw": float(new_remaining), "st": new_status, "id": new_card_id},
        )

    return new_card_id

def get_active_cards_by_phone(phone: str) -> pd.DataFrame:
    return run_query(
        """
        SELECT cards.*, members.name AS member_name, members.phone
        FROM cards
        JOIN members ON cards.member_id = members.id
        WHERE members.phone = :phone
          AND cards.remaining_weight > 0
        ORDER BY cards.purchase_date ASC, cards.id ASC
        """,
        params={"phone": phone}
    )

def choose_card_for_deduction(phone: str) -> Optional[Dict[str, Any]]:
    df = get_active_cards_by_phone(phone)
    if df.empty:
        return None
    return df.iloc[0].to_dict()

def deduct_card(card_id: int, weight: float, status: str = "成功扣卡", operator: str = "系统") -> Dict[str, Any]:
    with engine.begin() as connection:
        # ... (前面的代码保持不变) ...

        # 👑 新增：用来记录跨卡情报的变量
        cross_amount = 0.0
        cross_card_ids = []

        # 3. 跨卡自动抵扣逻辑
        if after_remain < 0:
            current_debt = -after_remain
            
            active_cards = connection.execute(
                # ... 这里的 SQL 保持不变 ...
            ).fetchall()

            for b_id, b_total, b_remaining in active_cards:
                if current_debt <= 0:
                    break
                    
                b_remaining = float(b_remaining)
                offset = min(current_debt, b_remaining)
                
                # ... (前面的更新 B 卡和插入 B 卡流水的代码保持不变) ...
                
                # 👑 记录跨卡情报
                cross_amount += offset
                cross_card_ids.append(str(b_id))
                
                current_debt -= offset
                after_remain += offset  # 把 A 卡的负债抹平
        
        # 4. 更新A卡最终的余额和状态
        new_status = compute_card_status(total, after_remain)
        connection.execute(
            text("UPDATE cards SET remaining_weight = :rw, status = :st WHERE id = :id"),
            {"rw": after_remain, "st": new_status, "id": card_id},
        )

    # 👑 修改返回值：把跨卡情报吐给前台
    return {
        "member_name": card["member_name"],
        "phone": card["phone"],
        "before_remain": before_remain,
        "after_remain": after_remain,
        "deduct_weight": float(weight),
        "card_id": card_id,
        "cross_amount": cross_amount,                 # 跨卡扣了多少斤
        "cross_card_ids": ", ".join(cross_card_ids)   # 扣的是哪几张卡
    }

def insert_retail_record(weight: float, status: str = "非会员零售", operator: str = "系统"):
    execute_query(
        """
        INSERT INTO records (
            card_id, member_id, op_date, delivery_date,
            weight, status, created_at, operator
        ) VALUES (
            NULL, NULL, (NOW() AT TIME ZONE 'PRC')::date, (NOW() AT TIME ZONE 'PRC')::date + INTERVAL '2 day',
            :weight, :status, (NOW() AT TIME ZONE 'PRC'), :operator
        )
        """,
        {"weight": float(weight), "status": status, "operator": operator},
    )

def update_record_weight(record_id: int, new_weight: float):
    with engine.begin() as connection:
        rec = connection.execute(
            text("SELECT * FROM records WHERE id = :id FOR UPDATE"),
            {"id": record_id},
        ).mappings().fetchone()
        if not rec:
            raise ValueError("Record not found")

        old_weight = float(rec["weight"])
        delta = float(new_weight) - old_weight

        connection.execute(
            text("UPDATE records SET weight = :w WHERE id = :id"),
            {"w": float(new_weight), "id": record_id},
        )

        card_id = rec["card_id"]
        if card_id is not None:
            card = connection.execute(
                text("SELECT * FROM cards WHERE id = :id FOR UPDATE"),
                {"id": int(card_id)},
            ).mappings().fetchone()
            if card:
                remaining = float(card["remaining_weight"])
                new_remaining = remaining - delta
                total = float(card["total_weight"])
                new_status = compute_card_status(total, new_remaining)
                connection.execute(
                    text(
                        "UPDATE cards SET remaining_weight = :rw, status = :st WHERE id = :id"
                    ),
                    {"rw": new_remaining, "st": new_status, "id": int(card_id)},
                )

def delete_record(record_id: int):
    with engine.begin() as connection:
        rec = connection.execute(
            text("SELECT * FROM records WHERE id = :id FOR UPDATE"),
            {"id": record_id},
        ).mappings().fetchone()
        if not rec:
            raise ValueError("Record not found")

        weight = float(rec["weight"])
        card_id = rec["card_id"]

        if card_id is not None:
            card = connection.execute(
                text("SELECT * FROM cards WHERE id = :id FOR UPDATE"),
                {"id": int(card_id)},
            ).mappings().fetchone()
            if card:
                remaining = float(card["remaining_weight"])
                new_remaining = remaining + weight
                total = float(card["total_weight"])
                new_status = compute_card_status(total, new_remaining)
                connection.execute(
                    text(
                        "UPDATE cards SET remaining_weight = :rw, status = :st WHERE id = :id"
                    ),
                    {"rw": new_remaining, "st": new_status, "id": int(card_id)},
                )

        connection.execute(text("DELETE FROM records WHERE id = :id"), {"id": record_id})

def query_records_with_join(date_field: str, start_date, end_date) -> pd.DataFrame:
    df = run_query(
        f"""
        SELECT
            records.*,
            members.name AS member_name,
            members.phone,
            cards.spec_kg_per_delivery,
            cards.cycle_type,
            cards.total_weight,
            cards.remaining_weight
        FROM records
        LEFT JOIN cards ON records.card_id = cards.id
        LEFT JOIN members ON records.member_id = members.id
        WHERE records.{date_field}::date BETWEEN :start AND :end
        ORDER BY records.{date_field}::date DESC, records.id DESC
        """,
        params={"start": start_date, "end": end_date}
    )

    # 👑 终极防御：强制将所有日期时间列转换为干净的字符串，彻底干掉 Streamlit Arrow 序列化报错！
    if not df.empty:
        # 处理纯日期
        for col in ['op_date', 'delivery_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d').replace('NaT', '')
        # 处理带时分秒的时间
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S').replace('NaT', '')

    return df
def get_member_cards(member_id: int) -> pd.DataFrame:
    return run_query(
        """
        SELECT *
        FROM cards
        WHERE member_id = :mid AND remaining_weight > 0
        ORDER BY purchase_date DESC, id DESC
        """,
        params={"mid": member_id}
    )

def get_recent_records(member_id: int, limit: int = 10) -> pd.DataFrame:
    return run_query(
        """
        SELECT *
        FROM records
        WHERE member_id = :mid
        ORDER BY op_date::date DESC, id DESC
        LIMIT :lim
        """,
        params={"mid": member_id, "lim": limit}
    )

def debt_cards() -> pd.DataFrame:
    return run_query(
        """
        SELECT cards.*, members.name AS member_name, members.phone
        FROM cards
        JOIN members ON cards.member_id = members.id
        WHERE cards.remaining_weight < 0
        ORDER BY cards.purchase_date ASC, cards.id ASC
        """
    )

def adjust_card_balance(card_id: int, weight_delta: float, reason: str, operator: str = "系统") -> Tuple[float, float]:
    """
    手动调整卡片余额（退补/冲销）
    weight_delta: 正数表示加回来（退还多扣的），负数表示额外扣除（补扣漏扣的）
    """
    with engine.begin() as connection:
        card = connection.execute(
            text("SELECT * FROM cards WHERE id = :card_id FOR UPDATE"),
            {"card_id": card_id},
        ).mappings().fetchone()
        
        if not card:
            raise ValueError("未找到该卡片")

        total = float(card["total_weight"])
        old_remain = float(card["remaining_weight"])
        new_remain = old_remain + weight_delta
        
        # 1. 更新卡片的新余额和新状态
        new_status = compute_card_status(total, new_remain)
        connection.execute(
            text("UPDATE cards SET remaining_weight = :rw, status = :st WHERE id = :id"),
            {"rw": new_remain, "st": new_status, "id": card_id},
        )
        
        # 2. 新增一条冲销/调整流水
        # 这里的 weight 在数据库代表“消耗了多少斤”。
        # 如果是退还斤数（weight_delta为正），说明消耗量是负的；如果是补扣（weight_delta为负），消耗量是正的。
        record_weight = -weight_delta 
        
        connection.execute(
            text(
                """
                INSERT INTO records (
                    card_id, member_id, op_date, delivery_date,
                    weight, status, created_at, operator
                ) VALUES (
                    :card_id, :member_id, (NOW() AT TIME ZONE 'PRC')::date, (NOW() AT TIME ZONE 'PRC')::date,
                    :weight, :status, (NOW() AT TIME ZONE 'PRC'), :operator
                )
                """
            ),
            {
                "card_id": card_id,
                "member_id": int(card["member_id"]),
                "weight": float(record_weight),
                "status": f"【财务调账】: {reason}",
                "operator": operator
            },
        )
        return old_remain, new_remain