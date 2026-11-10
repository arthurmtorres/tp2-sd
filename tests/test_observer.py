"""Regressões do observador. Execute após mvn package: python3 tests/test_observer.py."""
import csv, os, socket, subprocess, tempfile, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
JAVA=os.environ.get('JAVA','java')

def run(name, events, expected, code, barrier=False):
    with tempfile.TemporaryDirectory() as td:
        path=Path(td)/'summary.csv'
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0)); port=listener.getsockname()[1]
        args=[JAVA,'-Duser.language=pt','-Duser.country=BR','-cp',str(ROOT/'target/tp2-sync.jar'),'tp2.Observer','--nodes','2','--expected',str(expected),'--port',str(port),'--csv',str(path)]
        if barrier: args.append('--barrier')
        p=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        try:
            for _ in range(100):
                try: sock=socket.create_connection(('127.0.0.1',port),timeout=2); break
                except OSError:
                    if p.poll() is not None: raise AssertionError(p.communicate()[0])
                    time.sleep(.05)
            else: raise AssertionError('Observador não iniciou')
            with sock, sock.makefile('rwb') as f:
                for event, response in events:
                    f.write((event+'\n').encode());f.flush()
                    got=f.readline().decode().strip()
                    assert got.startswith(response),(name,event,got,response)
            output=p.communicate(timeout=5)[0]
            assert p.returncode==code,(name,p.returncode,output)
            with path.open() as f: rows=list(csv.DictReader(f))
            assert len(rows)==1 and None not in rows[0],rows
            return rows[0]
        finally:
            if p.poll() is None: p.kill();p.communicate()

def ok(*events): return [(e,'OK') for e in events]

def main():
    r=run('mediana e locale',ok('BEGIN n1 c1-1 1 10','END n1 c1-1 1','BEGIN n2 c2-1 1 20','END n2 c2-1 1','DONE n1','DONE n2'),2,0)
    assert float(r['median_wait_ms'])==15,r
    r=run('END sem BEGIN',[('END n1 c1-1 1','ERROR')]+ok('DONE n1','DONE n2'),1,2)
    assert r['completed']=='0' and r['protocol_violations']=='1',r
    r=run('duplicações',ok('BEGIN n1 c1-1 1 0')+[('BEGIN n1 c1-1 1 0','ERROR')]+ok('END n1 c1-1 1')+[('END n1 c1-1 1','ERROR')]+ok('DONE n1')+[('DONE n1','ERROR')]+ok('DONE n2'),1,2)
    assert r['completed']=='1' and r['protocol_violations']=='3',r
    r=run('sobreposição',ok('BEGIN n1 c1-1 1 0','BEGIN n2 c2-1 1 0','END n1 c1-1 1','END n2 c2-1 1','DONE n1','DONE n2'),2,2)
    assert r['overlaps']=='1',r
    valid=ok('ARRIVE n1 1','ARRIVE n2 1','PASS n1 1','PASS n2 1','BEGIN n1 c1-1 1 0','END n1 c1-1 1','LEAVE_READY n1 1','BEGIN n2 c2-1 1 0','END n2 c2-1 1','LEAVE_READY n2 1','LEFT n1 1','LEFT n2 1','DONE n1','DONE n2')
    run('barreira válida',valid,2,0,True)
    early=ok('ARRIVE n1 1')+[('ARRIVE n1 1','ERROR')]+ok('ARRIVE n2 1','PASS n1 1','PASS n2 1','BEGIN n1 c1-1 1 0','END n1 c1-1 1','LEAVE_READY n1 1','LEFT n1 1','BEGIN n2 c2-1 1 0','END n2 c2-1 1','LEAVE_READY n2 1','LEFT n2 1','DONE n1','DONE n2')
    r=run('saída prematura na última rodada',early,2,2,True)
    assert int(r['barrier_violations'])>=1 and int(r['protocol_violations'])>=1,r
    print('PASSOU: 6 regressões do observador, incluindo locale pt_BR e última barreira')

if __name__=='__main__': main()
