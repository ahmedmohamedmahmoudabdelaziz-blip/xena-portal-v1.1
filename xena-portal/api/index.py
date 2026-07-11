@app.route('/api/analytics', methods=['GET'])
def get_analytics():
    username = request.args.get('user', '').lower()
    uat = request.args.get('uat', '')
    if not uat:
        return jsonify({"error": "Unauthorized session. Please log in again."}), 401
    if not any(admin in username for admin in ADMIN_USERS):
        return jsonify({"error": "Unauthorized. Analytics are restricted to Administrators."}), 403

    tat = get_tenant_access_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {tat}", "Content-Type": "application/json"})

    # -- DEFAULT: region = PK (because all charts are for PK) --
    region_filter = request.args.get('region', 'PK').strip().lower()
    if region_filter == 'all':
        region_filter = 'pk'  # treat 'ALL' as PK for safety

    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')

    # -- DEFAULT: if no date range, use current month --
    if not date_from or not date_to:
        today = datetime.now()
        first_day = today.replace(day=1)
        last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        date_from = first_day.strftime("%Y-%m-%d")
        date_to = last_day.strftime("%Y-%m-%d")

    from_dt = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
    to_dt = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1) if date_to else None

    # ---- FETCH ALL RECORDS (with filters) ----
    req_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{REQUESTS_TABLE_ID}/records/search?automatic_fields=true"
    all_items = []
    seen_ids = set()
    page_token = ""
    error_msg = None

    # Try native filtering first
    conditions = []
    if region_filter not in ['all', '']:
        conditions.append({"field_name": "Region", "operator": "contains", "value": [region_filter.upper()]})
    if from_dt:
        from_ts = int(from_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        conditions.append({"field_name": "Submitted on", "operator": "isGreaterEqual", "value": [from_ts]})
    if to_dt:
        to_ts = int(to_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        conditions.append({"field_name": "Submitted on", "operator": "isLess", "value": [to_ts]})

    payload = {"page_size": 500}
    if conditions:
        payload["filter"] = {"conjunction": "and", "conditions": conditions}

    native_worked = False
    for _ in range(50):
        if page_token:
            payload["page_token"] = page_token
        res = session.post(req_url, json=payload, timeout=12).json()
        if res.get("code") != 0:
            break
        native_worked = True
        items = res.get("data", {}).get("items", [])
        for item in items:
            rid = item.get("record_id")
            if rid not in seen_ids:
                seen_ids.add(rid)
                all_items.append(item)
        page_token = res.get("data", {}).get("page_token")
        if not page_token:
            break

    # Fallback (if native filter fails)
    if not native_worked:
        all_items = []
        seen_ids = set()
        page_token = ""
        payload = {"page_size": 500}
        if region_filter not in ['all', '']:
            payload["filter"] = {"conjunction": "and", "conditions": [{"field_name": "Region", "operator": "contains", "value": [region_filter.upper()]}]}
        payload["sort"] = [{"field_name": "Submitted on", "desc": True}]
        for _ in range(50):
            if page_token:
                payload["page_token"] = page_token
            try:
                res = session.post(req_url, json=payload, timeout=12).json()
                if res.get("code") != 0:
                    break
                items = res.get("data", {}).get("items", [])
                new_records_in_page = 0
                stop_paginating = False
                for item in items:
                    rid = item.get("record_id")
                    if rid not in seen_ids:
                        seen_ids.add(rid)
                        all_items.append(item)
                        new_records_in_page += 1
                        if from_dt:
                            rec_dt = parse_feishu_date(get_field_local(item.get('fields', {}), 'Submitted on', 'Submitted on Copy'))
                            if rec_dt and rec_dt < from_dt:
                                stop_paginating = True
                if stop_paginating:
                    break
                if new_records_in_page == 0:
                    break
                page_token = res.get("data", {}).get("page_token")
                if not page_token:
                    break
            except Exception as e:
                error_msg = str(e)
                break

    # ---- PROCESS RECORDS (exact chart logic) ----
    stats = {
        "kpis": {"creations": 0, "bds": 0, "closings": 0},
        "creation_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "bd_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "closing_status": {"Done": 0, "Rejected": 0, "Under Investigation": 0},
        "acm_performance": {},
        "creation_types": {},
        "agency_types": {},
        "other_apps": {},
        "reject_reasons": {},
        "closing_reasons_pie": {},
        "acm_closing_reasons": {},
        "daily_trend": {},
        "scanned_rows": len(all_items),
        "error_debug": error_msg,
        "feishu_keys": []  # optional, can be omitted
    }

    # init daily trend with all days in range
    if from_dt and to_dt:
        cur = from_dt
        end = to_dt - timedelta(days=1)
        while cur <= end:
            stats["daily_trend"][cur.strftime("%Y-%m-%d")] = 0
            cur += timedelta(days=1)

    for item in all_items:
        fields = item.get('fields', {})
        record_dt = parse_feishu_date(get_field_local(fields, 'Submitted on', 'Submitted on Copy'))
        if not record_dt:
            continue
        if from_dt and record_dt < from_dt:
            continue
        if to_dt and record_dt >= to_dt:
            continue

        req_type = extract_field_text(get_field_local(fields, 'Request Type')).strip().lower()
        status_val = get_field_local(fields, 'Status', 'Request Status', 'Agency Status', 'State')
        status = extract_field_text(status_val).strip().lower()
        is_done = "done" in status or "complet" in status or "approv" in status
        is_rejected = "reject" in status or "fail" in status or "decline" in status

        region = extract_field_text(get_field_local(fields, 'Region', 'Agency Region')).strip().lower()
        if region_filter not in ['all', ''] and region != region_filter:
            continue  # already filtered globally, but keep safety

        # ---- KPI & Status Pies ----
        if "closing" in req_type:
            stats["kpis"]["closings"] += 1
            if is_done:
                stats["closing_status"]["Done"] += 1
            elif is_rejected:
                stats["closing_status"]["Rejected"] += 1
            else:
                stats["closing_status"]["Under Investigation"] += 1

            # Closing Reason Pie
            closing_reason = extract_field_text(get_field_local(fields, 'Closing Reason', 'Closing Agencies Reason')).strip()
            if closing_reason:
                stats["closing_reasons_pie"][closing_reason] = stats["closing_reasons_pie"].get(closing_reason, 0) + 1
                # ACM Closing Reasons Stacked
                acm_pk = extract_field_text(get_field_local(fields, 'Acm Name (PK)')).strip()
                if acm_pk:
                    clean_acm = acm_pk.title()
                    if clean_acm not in stats["acm_closing_reasons"]:
                        stats["acm_closing_reasons"][clean_acm] = {"User Request": 0, "Duplicated Hosting": 0}
                    if "user" in closing_reason.lower():
                        stats["acm_closing_reasons"][clean_acm]["User Request"] += 1
                    elif "dup" in closing_reason.lower():
                        stats["acm_closing_reasons"][clean_acm]["Duplicated Hosting"] += 1

        elif "bd" in req_type:
            stats["kpis"]["bds"] += 1
            if is_done:
                stats["bd_status"]["Done"] += 1
            elif is_rejected:
                stats["bd_status"]["Rejected"] += 1
            else:
                stats["bd_status"]["Under Investigation"] += 1

        else:  # Agency Creation (and anything else)
            stats["kpis"]["creations"] += 1
            if is_done:
                stats["creation_status"]["Done"] += 1
            elif is_rejected:
                stats["creation_status"]["Rejected"] += 1
            else:
                stats["creation_status"]["Under Investigation"] += 1

            # ---- ACM Onboarding (only Done) ----
            if is_done:
                acm_pk = extract_field_text(get_field_local(fields, 'Acm Name (PK)')).strip()
                if acm_pk:
                    clean_acm = acm_pk.title()
                    stats["acm_performance"][clean_acm] = stats["acm_performance"].get(clean_acm, 0) + 1

                # Other App Name (only Done and not empty)
                other_app = extract_field_text(get_field_local(fields, 'Otherapp Name', 'Other App Name', 'Other Apps')).strip()
                if other_app:
                    stats["other_apps"][other_app] = stats["other_apps"].get(other_app, 0) + 1

            # ---- Reject Reason (only if rejected and reason not empty) ----
            if is_rejected:
                reject_reason = extract_field_text(get_field_local(fields, 'Reject Reason', 'Rejection Reason', 'Agencies Rejection Reason')).strip()
                if reject_reason:
                    stats["reject_reasons"][reject_reason] = stats["reject_reasons"].get(reject_reason, 0) + 1

            # ---- Creation Type ----
            creation_type = extract_field_text(get_field_local(fields, 'Create Way', 'Creation Type', 'Agency Creation Type')).strip()
            if creation_type:
                stats["creation_types"][creation_type] = stats["creation_types"].get(creation_type, 0) + 1

            # ---- Agency Type ----
            agency_type = extract_field_text(get_field_local(fields, 'Agency Type', 'Type of Agency')).strip()
            if agency_type:
                clean_type = agency_type.title()
                stats["agency_types"][clean_type] = stats["agency_types"].get(clean_type, 0) + 1

        # ---- Daily Trend (only Done, all request types) ----
        if is_done and record_dt:
            date_str = record_dt.strftime("%Y-%m-%d")
            if date_str in stats["daily_trend"]:
                stats["daily_trend"][date_str] += 1

    # sort descending
    stats["acm_performance"] = dict(sorted(stats["acm_performance"].items(), key=lambda x: x[1], reverse=True))
    stats["reject_reasons"] = dict(sorted(stats["reject_reasons"].items(), key=lambda x: x[1], reverse=True))
    stats["closing_reasons_pie"] = dict(sorted(stats["closing_reasons_pie"].items(), key=lambda x: x[1], reverse=True))
    stats["other_apps"] = dict(sorted(stats["other_apps"].items(), key=lambda x: x[1], reverse=True))
    stats["creation_types"] = dict(sorted(stats["creation_types"].items(), key=lambda x: x[1], reverse=True))
    stats["agency_types"] = dict(sorted(stats["agency_types"].items(), key=lambda x: x[1], reverse=True))
    stats["daily_trend"] = dict(sorted(stats["daily_trend"].items()))

    return jsonify(stats)
