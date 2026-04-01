import json
from datetime import datetime

results = {}

# Akshare
try:
    import akshare as ak
    results['akshare_import'] = 'ok'
    try:
        df = ak.stock_zh_a_hist(symbol='000001', period='daily', start_date='20240101', end_date='20240131', adjust='qfq')
        results['akshare_stock_zh_a_hist'] = {
            'ok': True,
            'rows': int(len(df)),
            'cols': list(df.columns)[:10],
        }
    except Exception as e:
        results['akshare_stock_zh_a_hist'] = {'ok': False, 'error': str(e)}
except Exception as e:
    results['akshare_import'] = {'ok': False, 'error': str(e)}

# Tushare
try:
    import tushare as ts
    results['tushare_import'] = 'ok'
    try:
        pro = ts.pro_api()
        results['tushare_pro_api'] = {'ok': True, 'note': 'client_created'}
    except Exception as e:
        results['tushare_pro_api'] = {'ok': False, 'error': str(e)}
except Exception as e:
    results['tushare_import'] = {'ok': False, 'error': str(e)}

payload = {
    'checked_at': datetime.now().isoformat(timespec='seconds'),
    'results': results,
}
print(json.dumps(payload, ensure_ascii=False, indent=2))
