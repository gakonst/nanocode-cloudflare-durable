import json,re,sys
sys.path.insert(0,'scripts')
from importlib.machinery import SourceFileLoader
def norm(s):
    s=re.sub(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}','<uuid>',s)
    s=re.sub(r'(spawned_at|finished_at|started_at|updated_at|created_at|_ms|_at)(\\*"):\s*[0-9.]+',r'\1\2:N',s)
    s=re.sub(r'Wall time [0-9.]+ seconds','Wall time T',s)
    return s
a=json.load(open(sys.argv[1])); b=json.load(open(sys.argv[2]))
for who in ['root','A','B','C','host']:
    xa=[norm(json.dumps(e,sort_keys=True)) for e in a['log'] if e['who']==who]
    xb=[norm(json.dumps(e,sort_keys=True)) for e in b['log'] if e['who']==who]
    diffs=[(i,x,y) for i,(x,y) in enumerate(zip(xa,xb)) if x!=y]
    print(who,len(xa),len(xb),'diffs',len(diffs))
    for i,x,y in diffs: print('  ',i,'\n   IDO   ',x[:600],'\n   REMOTE',y[:600])
