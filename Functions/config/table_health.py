tables_config = {
    "tables": {
        "account_level": {},
        "cash_movement": {},
        "corporate_actions": {},
        "daily_transaction": {},
        "dpr": {},
        "interest_account": {},
        "master_instrument": {},
        "pos": {},
        "position_overview": {},
        "positions_synthetics": {},
        "trade_synthetics": {},
        "trx": {}
    },
    "tables_dependencies": {
        "clearing": {"name": ["ctr", "haircut"]},
        "daily_pl": {"name": ["amendments", "amdax", "fencer", "finst"]},
        "files": {
            "account_treecap": [
                "FF", "FF_A", "FF_B", "FF_C", "FF_E", "FF_F", "FF_G",
                "TCAF_C", "TREECAP"
            ]
        }
    }
}