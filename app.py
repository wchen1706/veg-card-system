import streamlit as st
st.set_page_config(
        page_title="蔬菜配送会员管理系统",
        layout="wide",
    )
# ======== 👑 新增：UI 极简美化魔法 ========
def inject_custom_css():
    st.markdown("""
        <style>
        /* 1. 隐藏 Streamlit 默认的右上角菜单和底部水印 (显得更像独立开发的 App) */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* 2. 优化顶部留白，让页面内容更紧凑 */
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        
        /* 3. 美化表单和卡片边框，增加轻微的高级阴影 */
        div[data-testid="stForm"] {
            border: 1px solid #f0f2f6;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            padding: 20px;
        }
        
        /* 4. 让主要的按钮显得更立体饱满 */
        button[kind="primary"] {
            border-radius: 8px;
            font-weight: bold;
            transition: all 0.3s ease;
        }
        button[kind="primary"]:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        </style>
    """, unsafe_allow_html=True)

# 调用美化魔法
inject_custom_css()

import pandas as pd
from datetime import datetime
from io import StringIO
from typing import List, Tuple

import query as q

from sqlalchemy import create_engine, text

# =====================
# 数据库初始化与基础函数
# =====================

def init_db():
    # PostgreSQL 在云端由你执行 SQL 初始化，这里无需本地初始化
    return


# =====================
# 通用数据库操作封装
# =====================

compute_card_status = q.compute_card_status


# =====================
# B 端：模块 1 – 开卡与会员管理
# =====================

def page_open_card_manage():
    st.subheader("模块1：开卡与会员管理")

    with st.form("open_card_form"):
        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("真实姓名", "")
            wechat_name = st.text_input("微信名/备注", "")
        with col2:
            phone = st.text_input("手机号（唯一）", "")

            purchase_date = st.date_input(
                "购卡日期", value=datetime.now().date()
            )

        st.markdown("**菜卡规格选择**")
        spec_option = st.selectbox(
            "选择卡片规格与周期",
            [
                "6斤 月卡 (4次)",
                "6斤 季卡 (12次)",
                "6斤 年卡 (50次)",
                "8斤 月卡 (4次)",
                "8斤 季卡 (12次)",
                "8斤 年卡 (50次)",
            ],
        )

        submitted = st.form_submit_button("确认开卡/续卡")

        if submitted:
            if not name or not phone:
                st.error("姓名与手机号为必填项。")
            else:
                # 解析规格
                if spec_option.startswith("6斤"):
                    spec_kg = 6
                else:
                    spec_kg = 8

                if "月卡" in spec_option:
                    cycle_type = "month"
                elif "季卡" in spec_option:
                    cycle_type = "quarter"
                else:
                    cycle_type = "year"

                # 判断会员是否存在
                existing_member = q.get_member_by_phone(phone)
                if existing_member:
                    member_id = int(existing_member["id"])
                    st.info(f"检测到老会员：{existing_member['name']}，将在其名下新增菜卡。")
                else:
                    member_id = q.create_member(name, wechat_name, phone)
                    st.success(f"已创建新会员档案：{name}。")

                # 创建菜卡
                purchase_dt = datetime.combine(purchase_date, datetime.min.time())
                card_id = q.create_card_with_debt_fill(member_id, spec_kg, cycle_type, purchase_dt.date())

                st.success(f"开卡/续卡成功！卡片ID：{card_id}")


# =====================
# B 端：模块 2 – 批量/手动 配送扣卡
# =====================

def parse_pasted_table(text: str) -> Tuple[pd.DataFrame, List[str]]:
    errors = []
    if not text.strip():
        errors.append("请先在文本框中粘贴内容。")
        return pd.DataFrame(), errors

    buffer = StringIO(text)

    # 尝试多种分隔符
    parsed = None
    for sep in ["\t", ",", r"\s+"]:
        try:
            buffer.seek(0)
            parsed = pd.read_csv(buffer, sep=sep, engine="python")
            if parsed.shape[1] >= 3:
                break
        except Exception:
            parsed = None

    if parsed is None or parsed.shape[1] < 3:
        errors.append("无法识别粘贴内容，请确认为 3 列（姓名、手机号、实发斤数），可为 Excel 复制结果。")
        return pd.DataFrame(), errors

    # 尝试列名映射
    cols = list(parsed.columns)
    mapping = {}

    def find_col(candidates):
        for c in cols:
            for cand in candidates:
                if cand in str(c):
                    return c
        return None

    name_col = find_col(["姓名", "name", "客户"])
    phone_col = find_col(["手机号", "电话", "phone"])
    weight_col = find_col(["实发斤数", "重量", "斤数", "weight"])

    if name_col is None or phone_col is None or weight_col is None:
        if len(cols) >= 3:
            name_col, phone_col, weight_col = cols[0], cols[1], cols[2]
        else:
            errors.append("无法识别列名，请确保包含 姓名、手机号、实发斤数。")
            return pd.DataFrame(), errors

    df = parsed[[name_col, phone_col, weight_col]].copy()
    df.columns = ["name", "phone", "weight"]
    # 清洗
    df["phone"] = df["phone"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    try:
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
    except Exception:
        df["weight"] = None

    df = df.dropna(subset=["weight"])
    return df, errors


def batch_deduction_ui():
    st.markdown("#### 批量粘贴扣卡")

    text = st.text_area(
        "从 Excel 直接复制 3 列数据粘贴到这里（表头：姓名、手机号、实发斤数）",
        height=180,
        key="batch_paste_input",
    )

    if "batch_success_df" not in st.session_state:
        st.session_state.batch_success_df = pd.DataFrame()
    if "batch_error_df" not in st.session_state:
        st.session_state.batch_error_df = pd.DataFrame()

    if st.button("解析并匹配菜卡"):
        df, errs = parse_pasted_table(text)
        if errs:
            for e in errs:
                st.error(e)
            return

        if df.empty:
            st.warning("没有有效数据行。")
            return

        success_rows = []
        error_rows = []

        for _, row in df.iterrows():
            name = row["name"]
            phone = str(row["phone"]).strip()
            weight = float(row["weight"])

            card = q.choose_card_for_deduction(phone)
            if card is None:
                error_rows.append(
                    {
                        "姓名": name,
                        "手机号": phone,
                        "实发斤数": weight,
                        "异常原因": "未找到有效菜卡",
                    }
                )
                continue

            remaining = float(card["remaining_weight"])
            after_remaining = remaining - weight
            success_rows.append(
                {
                    "姓名": name,
                    "手机号": phone,
                    "实发斤数": weight,
                    "卡ID": card["id"],
                    "卡型": f"{card['spec_kg_per_delivery']}斤-{card['cycle_type']}",
                    "剩余斤数(扣前)": remaining,
                    "预计剩余斤数(扣后)": after_remaining,
                }
            )

        st.session_state.batch_success_df = pd.DataFrame(success_rows)
        st.session_state.batch_error_df = pd.DataFrame(error_rows)

    if not st.session_state.batch_success_df.empty:
        st.markdown("##### 待扣款区（已匹配菜卡，允许扣超）")
        df_show = st.session_state.batch_success_df.copy()
        overdraft_rows = df_show[df_show["预计剩余斤数(扣后)"] < 0]
        if not overdraft_rows.empty:
            st.markdown(
                f"<span style='color:red'>提示：本次待扣款中有 {len(overdraft_rows)} 条将产生欠费（扣后为负数）。</span>",
                unsafe_allow_html=True,
            )
        st.dataframe(df_show, use_container_width=True)

        if st.button("一键确认扣款"):
            df_success = st.session_state.batch_success_df
            if df_success.empty:
                st.info("暂无待扣款记录。")
            else:
                success_count = 0
                overdraft_happened = False
                st.markdown("**本次扣卡明细核对：**")
                for _, row in df_success.iterrows():
                    card_id = int(row["卡ID"])
                    weight = float(row["实发斤数"])
                    before_remain = float(row["剩余斤数(扣前)"])
                    after_remain = before_remain - weight
                    try:
                        q.deduct_card(card_id, weight, status="成功扣卡",operator=st.session_state.operator)
                        success_count += 1
                        st.write(
                            f"会员：{row['姓名']} / {row['手机号']} | 扣前：{before_remain:.2f} 斤 | 扣除：{weight:.2f} 斤 | 扣后：{after_remain:.2f} 斤"
                        )
                        if after_remain < 0:
                            overdraft_happened = True
                            st.markdown(
                                f"<span style='color:red'>该次扣卡已产生欠费 {abs(after_remain):.2f} 斤，请及时提醒客户续卡。</span>",
                                unsafe_allow_html=True,
                            )
                    except Exception as e:
                        st.error(f"卡ID {card_id} 扣款失败：{e}")

                st.success(f"已成功处理 {success_count} 条扣款记录。")
                if overdraft_happened:
                    st.markdown(
                        "<span style='color:red'>本次批量扣卡中存在欠费情况，请优先关注红色标记的会员。</span>",
                        unsafe_allow_html=True,
                    )
                st.session_state.batch_success_df = pd.DataFrame()

    if not st.session_state.batch_error_df.empty:
        st.markdown("##### 异常区（未找到匹配菜卡）")
        error_df = st.session_state.batch_error_df.copy()

        not_found_count = (error_df["异常原因"] == "未找到有效菜卡").sum()
        if not_found_count:
            st.warning(
                f"共有 {not_found_count} 条记录未找到匹配的会员/菜卡，请核对手机号或是否已开卡。"
            )

        st.dataframe(error_df, use_container_width=True)

        st.markdown("**对异常记录可以逐条处理：**")
        new_error_rows = []
        for idx, row in error_df.iterrows():
            cols = st.columns([3, 1, 1])
            with cols[0]:
                st.write(
                    f"{row['姓名']} / {row['手机号']} / {row['实发斤数']} 斤 —— {row['异常原因']}"
                )
            with cols[1]:
                ignore = st.button("忽略", key=f"ignore_{idx}")
            with cols[2]:
                retail = st.button("散客单买", key=f"retail_{idx}")

            if retail:
                q.insert_retail_record(float(row["实发斤数"]), status="非会员零售", operator=st.session_state.operator)
                st.success(f"已作为散客单买记录写入：{row['姓名']} / {row['实发斤数']}斤")
            elif ignore:
                st.info(f"已忽略：{row['姓名']} / {row['手机号']}")
            else:
                new_error_rows.append(row)

        st.session_state.batch_error_df = pd.DataFrame(new_error_rows)

        if st.session_state.batch_error_df.empty:
            st.info("当前异常记录已全部处理。")


def manual_deduction_ui():
    st.markdown("#### 💳 单独手动扣卡")

    df_cards = q.run_query(
        """
        SELECT cards.*, members.name AS member_name, members.phone, members.wechat_name
        FROM cards
        JOIN members ON cards.member_id = members.id
        WHERE cards.remaining_weight > 0
        ORDER BY members.name ASC, cards.purchase_date ASC, cards.id ASC
        """
    )

    if df_cards.empty:
        st.info("当前没有可用的菜卡。")
        return

    # 1. 移动端杀手锏：真正的文本搜索框！点这里唤起手机键盘！
    search_kw = st.text_input("🔍 输入姓名、微信或手机号快速筛选：", "")

    options = []
    for _, r in df_cards.iterrows():
        rem_w = float(r["remaining_weight"])
        wechat = r.get("wechat_name", "未填")
        phone = r["phone"]
        name = r["member_name"]
        
        # 2. 极限压缩排版！去掉所有冗余汉字，用符号分隔，防止下拉框截断
        # 展示效果 👉 王大拿(dana88,13800138000) | 剩10斤 | 5斤/次 | 卡12
        display = f"{name}({wechat},{phone}),剩{rem_w}斤|{r['spec_kg_per_delivery']}斤|卡{r['id']}"
        
        # 3. 智能过滤
        if search_kw and search_kw not in display:
            continue
            
        options.append((display, r.to_dict()))

    if not options:
        st.warning("👻 没有找到匹配的会员，请检查搜索词。")
        return

    labels = [o[0] for o in options]
    
    # 4. 换回你熟悉的下拉框！现在因为字数精简了，手机上大概率能完整显示！
    selected_label = st.selectbox("👇 请选择要扣除的菜卡", labels)
    
    selected_row = None
    for label, r in options:
        if label == selected_label:
            selected_row = r
            break

    st.markdown("---")
    weight = st.number_input("⚖️ 实发斤数", min_value=0.0, step=0.5, value=0.0)

    if st.button("✅ 确认手动扣卡"):
        if weight <= 0:
            st.error("实发斤数必须大于 0。")
            return

        card_id = selected_row["id"]
        spec_kg = int(selected_row["spec_kg_per_delivery"])

        if weight < spec_kg:
            diff = spec_kg - weight
            st.warning(f"⚠️ 少点 {diff:.2f} 斤，请提醒客户确认。")

        try:
            res = q.deduct_card(card_id, weight, status="手动扣卡", operator=st.session_state.operator)
            real_after = res["after_remain"]
            cross_amt = res.get("cross_amount", 0.0)      # 拿到跨卡斤数
            cross_ids = res.get("cross_card_ids", "")     # 拿到备用卡号
            
            st.toast("扣卡成功！", icon="✅")
            
            # 👑 全新智能分级提示
            if cross_amt > 0:
                # 触发了跨卡抵扣
                st.warning(
                    f"🔄 **跨卡抵扣触发**：本次共扣 {weight} 斤。当前卡片已用完，"
                    f"超出的 **{cross_amt:.2f} 斤** 已自动从该会员的备用卡 (卡号: {cross_ids}) 中安全扣除！"
                )
            elif real_after < 0:
                # 连备用卡都不够扣，产生真正的欠费
                st.error(f"🚨 **警报**：该会员所有备用卡均已扣空！当前产生真实欠费 **{abs(real_after):.2f} 斤**，请务必提醒客户续费。")
            elif real_after == 0:
                # 刚好扣完
                st.success(f"✅ 成功扣除：{weight:.2f} 斤。")
                st.warning("⚠️ **提醒**：该卡片刚才已刚好用完（余额 0 斤），请提醒客户下次准备续费。")
            else:
                # 正常扣除还有结余
                st.success(f"✅ 成功扣除：{weight:.2f} 斤 | 会员：{res['member_name']} | 最新剩余：{real_after:.2f} 斤")
                
        except Exception as e:
            st.error(f"❌ 扣卡失败：{e}")


def admin_db_browser():
    st.markdown("#### 数据库原始数据浏览")

    table = st.selectbox("选择数据表", ["records", "cards", "members"], index=0)
    search = st.text_input("搜索关键字（支持模糊匹配，作用于当前表所有字段）", "")

    # 替换为 q.run_query
    if table == "members":
        df = q.run_query("SELECT * FROM members ORDER BY id DESC")
    elif table == "cards":
        df = q.run_query(
            """
            SELECT cards.*, members.name AS member_name, members.phone
            FROM cards
            JOIN members ON cards.member_id = members.id
            ORDER BY cards.purchase_date ASC, cards.id ASC
            """
        )
    else:  # records
        df = q.run_query(
            """
            SELECT records.*, members.name AS member_name, members.phone
            FROM records
            LEFT JOIN members ON records.member_id = members.id
            ORDER BY records.id DESC
            """
        )

    if not df.empty and search.strip():
        mask = df.astype(str).apply(
            lambda col: col.str.contains(search.strip(), case=False, na=False)
        )
        df_display = df[mask.any(axis=1)].copy()
    else:
        df_display = df.copy()

    if table == "cards" and not df_display.empty:
        df_display["卡片状态"] = df_display.apply(
            lambda r: compute_card_status(
                float(r["total_weight"]), float(r["remaining_weight"])
            ),
            axis=1,
        )

    st.dataframe(df_display, use_container_width=True)


def edit_records_ui():
    st.markdown("#### ⚖️ 历史流水与退补调账")
    st.info("💡 财务规范：系统已禁止直接篡改或删除历史流水。如遇错扣、漏扣，请通过下方的「调账」功能为该卡片新增一笔冲销记录，做到账目绝对可追溯。")

    # ==========================================
    # 第一部分：保留你原有的优秀功能 —— 查账表
    # ==========================================
    st.markdown("##### 1. 近期扣卡流水 (仅供查阅对账)")
    search = st.text_input("🔍 搜索历史流水（手机号 / 姓名 / 状态等）", "")

    df = q.run_query(
        """
        SELECT records.*, members.name AS member_name, members.phone
        FROM records
        LEFT JOIN members ON records.member_id = members.id
        ORDER BY records.id DESC LIMIT 200
        """
    )

    if df.empty:
        st.info("当前暂无扣卡流水记录。")
    else:
        if search.strip():
            mask = df.astype(str).apply(
                lambda col: col.str.contains(search.strip(), case=False, na=False)
            )
            df_display = df[mask.any(axis=1)].copy()
        else:
            df_display = df.copy()

        # 展示数据表格
        st.dataframe(df_display, use_container_width=True)

    st.markdown("---")

    # ==========================================
    # 第二部分：全新的正规退补调账引擎
    # ==========================================
    st.markdown("##### 2. 💳 卡片余额退补调账")
    
    # 这里的 SQL 不加 remaining_weight > 0 的限制，因为用完的卡也可能需要退还斤数
    df_cards = q.run_query(
        """
        SELECT cards.*, members.name AS member_name, members.phone, members.wechat_name
        FROM cards
        JOIN members ON cards.member_id = members.id
        ORDER BY members.name ASC, cards.id DESC
        """
    )

    if df_cards.empty:
        st.warning("当前没有卡片记录，无法调账。")
        return

    search_kw = st.text_input("🔍 搜索要调账的会员菜卡 (姓名/手机/微信)：", "", key="search_adj")

    options = []
    for _, r in df_cards.iterrows():
        rem_w = float(r["remaining_weight"])
        wechat = r.get("wechat_name", "未填")
        phone = r["phone"]
        name = r["member_name"]
        
        # 继续沿用你最喜欢的极简排版
        display = f"{name}({wechat},{phone}),剩{rem_w}斤|卡{r['id']}"
        
        if search_kw and search_kw not in display:
            continue
        options.append((display, r.to_dict()))

    if not options:
        st.warning("👻 未找到匹配的卡片。")
        return

    labels = [o[0] for o in options]
    selected_label = st.selectbox("👇 选择需要调账的菜卡", labels, key="select_adj")
    
    selected_row = None
    for label, r in options:
        if label == selected_label:
            selected_row = r
            break
    
    col1, col2 = st.columns(2)
    with col1:
        adj_type = st.radio("🔄 调账类型", ["➕ 退还斤数 (把多扣的还回去)", "➖ 补扣斤数 (把漏扣的补回来)"])
    with col2:
        adj_amount = st.number_input("⚖️ 调整斤数 (绝对值)", min_value=0.0, step=0.5, value=0.0)

    reason = st.text_input("📝 调账原因备注 (必填项，例：刚才多扣了0.5斤，现退回)", "")

    if st.button("⚖️ 确认生成调账凭证"):
        if adj_amount <= 0:
            st.error("调整斤数必须大于 0！")
            return
        if not reason.strip():
            st.error("🚨 请务必填写调账/修改原因，以备核查！")
            return

        weight_delta = adj_amount if "退还" in adj_type else -adj_amount
        try:
            old_r, new_r = q.adjust_card_balance(
                card_id=selected_row["id"], 
                weight_delta=weight_delta, 
                reason=reason.strip(),
                operator=st.session_state.operator
            )
            st.toast("调账成功！", icon="✅")
            st.success(f"✅ 冲销记录已生成！该卡原余额: **{old_r:.2f} 斤** ➡️ 最新余额: **{new_r:.2f} 斤**。流水已永久记录。")
            
        except Exception as e:
            st.error(f"❌ 修改/调账失败: {e}")


def page_batch_and_manual_deduction():
    st.subheader("模块2：批量/手动 配送扣卡")

    tab1, tab2, tab3 = st.tabs(
        ["单独手动扣卡", "批量粘贴扣卡", "修改历史扣卡记录"]
    )

    with tab1:
        manual_deduction_ui()
    with tab2:
        batch_deduction_ui()
    with tab3:
        edit_records_ui()


# =====================
# B 端：模块 3 – 数据看板与日结汇总
# =====================

def page_dashboard():
    st.subheader("模块3：数据看板与日结汇总")

    mode = st.radio(
        "按日期维度筛选",
        ["按扣卡日期", "按配送日期"],
        horizontal=True,
    )

    today = datetime.now().date()
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("开始日期", value=today)
    with col2:
        end_date = st.date_input("结束日期", value=today)

    if start_date > end_date:
        st.error("开始日期不能晚于结束日期。")
        return

    date_field = "op_date" if mode == "按扣卡日期" else "delivery_date"
    df = q.query_records_with_join(date_field, start_date, end_date)

    if df.empty:
        st.info("所选日期范围内暂无记录。")
        return

    # 顶部指标
    total_orders = len(df)
    total_weight = df["weight"].sum()

    m1, m2 = st.columns(2)
    m1.metric("总订单单数", f"{total_orders}")
    m2.metric("总发货斤数", f"{total_weight:.2f} 斤")

    if "total_weight" in df.columns and "remaining_weight" in df.columns:
        df["card_status"] = df.apply(
            lambda r: compute_card_status(
                float(r["total_weight"]) if pd.notna(r["total_weight"]) else 0.0,
                float(r["remaining_weight"])
                if pd.notna(r["remaining_weight"])
                else 0.0,
            ),
            axis=1,
        )
    else:
        df["card_status"] = ""

    display_df = df[
        [
            date_field,
            "delivery_date",
            "member_name",
            "phone",
            "weight",
            "status",
            "card_id",
            "spec_kg_per_delivery",
            "cycle_type",
            "card_status",
        ]
    ].rename(
        columns={
            date_field: "日期",
            "delivery_date": "配送日期",
            "member_name": "姓名",
            "phone": "手机号",
            "weight": "实发斤数",
            "status": "扣除状态",
            "card_id": "菜卡ID",
            "spec_kg_per_delivery": "单次规格(斤)",
            "cycle_type": "卡片周期",
            "card_status": "卡片状态",
        }
    )

    st.markdown("#### 明细列表")
    # 👑 终极清理：剔除所有重复的列名（保留第一个），专治 Arrow 引擎崩溃！
    display_df = display_df.loc[:, ~display_df.columns.duplicated()]
    st.dataframe(display_df, use_container_width=True)
    


def page_db_admin():
    st.subheader("模块4：数据库原始表查询")
    admin_db_browser()


def page_debt_reminder():
    st.subheader("模块5：欠费续卡提醒")
    df = q.debt_cards()

    if df.empty:
        st.info("当前暂无欠费菜卡。")
        return

    df["total_weight"] = pd.to_numeric(
        df.get("total_weight", 0), errors="coerce"
    ).fillna(0)
    df["remaining_weight"] = pd.to_numeric(
        df.get("remaining_weight", 0), errors="coerce"
    ).fillna(0)

    df["欠费斤数"] = df["remaining_weight"].apply(lambda x: abs(float(x)))
    df["卡片状态"] = df.apply(
        lambda r: compute_card_status(
            float(r["total_weight"]), float(r["remaining_weight"])
        ),
        axis=1,
    )

    display_df = df[
        [
            "member_name",
            "phone",
            "id",
            "spec_kg_per_delivery",
            "cycle_type",
            "total_weight",
            "remaining_weight",
            "欠费斤数",
            "卡片状态",
            "purchase_date",
        ]
    ].rename(
        columns={
            "member_name": "姓名",
            "phone": "手机号",
            "id": "菜卡ID",
            "spec_kg_per_delivery": "单次规格(斤)",
            "cycle_type": "卡片周期",
            "total_weight": "总斤数",
            "remaining_weight": "当前剩余斤数",
            "purchase_date": "购卡日期",
        }
    )

    st.markdown("#### 欠费会员列表（按购卡时间排序）")
    st.error("以下会员存在欠费（红色行），请尽快联系续卡。")

    tmp = display_df.copy()
    tmp.insert(0, "标记", "欠费")

    def esc(x):
        return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    headers = "".join([f"<th style='padding:6px;border:1px solid #ddd'>{esc(c)}</th>" for c in tmp.columns])
    rows_html = ""
    for _, r in tmp.iterrows():
        rows_html += "<tr style='background-color:#ffcccc'>"
        for c in tmp.columns:
            rows_html += f"<td style='padding:6px;border:1px solid #ddd'>{esc(r[c])}</td>"
        rows_html += "</tr>"

    html = f"""
    <div style="overflow:auto;max-height:520px;border:1px solid #eee">
      <table style="border-collapse:collapse;width:100%;font-size:14px">
        <thead><tr style="position:sticky;top:0;background:#fafafa">{headers}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


# =====================
# 主应用入口
# =====================

def main():
    # ======= 👑 登录拦截器开始 =======
    if "operator" not in st.session_state:
        st.session_state.operator = None

    # 如果没有登录（没选员工），就只显示登录页，隐藏整个系统
    if not st.session_state.operator:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown("<h2 style='text-align: center;'>🥬门店管理系统</h2>", unsafe_allow_html=True)
        st.markdown("<h5 style='text-align: center; color: gray;'>请选择您的操作员身份进入系统</h5>", unsafe_allow_html=True)
        
        # 居中排版
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            # 这里的名字你可以随便改成你们真实的店员名字
            staff_list = ["👨‍💼 店长 (glj)", "👩‍🌾 店员 (ccc)", "🧑‍💻 店员 (glx)"]
            selected_staff = st.selectbox("当前值班人员", staff_list)

            # 🔐 新增：授权码输入框（隐藏输入内容）
            auth_code = st.text_input("请输入门店授权码", type="password")
            
            # 从云端保险箱取密码，如果取不到，默认用一个备用的
            correct_password = st.secrets.get("auth_password", "admin123")
            
            if st.button("🚀 登 入 系 统", use_container_width=True):
                # 这里设置一个你自己才知道的暗号，比如 'ssy888'
                if auth_code == correct_password: 
                    st.session_state.operator = selected_staff
                    st.rerun()
                else:
                    st.error("❌ 授权码错误，无法进入系统")
        return # 核心：直接 return，不让后面的侧边栏和主菜单加载出来
    # ======= 👑 登录拦截器结束 =======

    # ======= 已登录状态 =======
    # 增加退出登录和当前身份展示
    st.sidebar.markdown(f"**🟢 当前在线：{st.session_state.operator}**")
    if st.sidebar.button("🚪 退出登录"):
        st.session_state.operator = None
        st.rerun()
    st.sidebar.markdown("---")
    

    st.sidebar.title("蔬菜配送会员管理系统")

    # 注意：在重构后，这里只需要跑 B 端后台，因为 C 端我们分离到独立文件了。
    # 为了防止你这里报错，我已经把 C 端入口去掉了，这是纯粹的老板后台！
    module = st.sidebar.radio(
        "选择模块",
        [
            "模块1：开卡与会员管理",
            "模块2：批量/手动 配送扣卡",
            "模块3：数据看板与日结汇总",
            "模块4：数据库原始表查询",
            "模块5：欠费续卡提醒",
        ],
    )

    if module.startswith("模块1"):
        page_open_card_manage()
    elif module.startswith("模块2"):
        page_batch_and_manual_deduction()
    elif module.startswith("模块3"):
        page_dashboard()
    elif module.startswith("模块4"):
        page_db_admin()
    elif module.startswith("模块5"):
        page_debt_reminder()

if __name__ == "__main__":
    main()