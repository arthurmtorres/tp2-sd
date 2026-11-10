package tp2;

import java.io.*;
import java.net.*;
import java.util.*;

/** Observa eventos, sem conceder permissões ou impedir BEGIN concorrentes. */
public class Observer {
    private record Access(String node, int round) {}
    private static final Object L = new Object();
    private static int n, expected, maxActive, overlaps, barrierViol, protocolViol;
    private static boolean barrierMode, finishing;
    private static long first = -1, last = -1;
    private static String label, csv, seed, runId;
    private static final Set<String> nodes = new HashSet<>(), done = new HashSet<>();
    private static final Map<String, Access> active = new HashMap<>();
    private static final Set<String> completed = new HashSet<>();
    private static final Map<Integer, Set<String>> arrivals = new HashMap<>(), passed = new HashMap<>(),
            ended = new HashMap<>(), ready = new HashMap<>(), left = new HashMap<>();
    private static final Map<String, Integer> latest = new HashMap<>(), perClient = new TreeMap<>();
    private static final List<Long> waits = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        Map<String,String> o = Util.parseArgs(args);
        n = Integer.parseInt(o.get("nodes"));
        expected = Integer.parseInt(o.get("expected"));
        if (n < 1 || expected < 1) throw new IllegalArgumentException("nodes e expected devem ser positivos");
        for (int i=1;i<=n;i++) nodes.add("n"+i);
        barrierMode = o.containsKey("barrier");
        label=o.getOrDefault("label","run"); seed=o.getOrDefault("seed","0");
        runId=o.getOrDefault("run-id", label+"-"+seed);
        if (!runId.matches("[A-Za-z0-9_.-]+") || !label.matches("[A-Za-z0-9_.-]+") || !seed.matches("[0-9]+"))
            throw new IllegalArgumentException("Identificadores invalidos para CSV");
        csv=o.getOrDefault("csv","results/summary.csv");
        try (ServerSocket ss=new ServerSocket(Integer.parseInt(o.getOrDefault("port","9000")))) {
            Util.log("OBS","LISTENING","nodes="+n+" barrier="+barrierMode);
            while(true) { Socket s=ss.accept(); Thread t=new Thread(()->handle(s)); t.setDaemon(true); t.start(); }
        }
    }
    private static Set<String> group(Map<Integer,Set<String>> m,int r) {
        return m.computeIfAbsent(r,k->new HashSet<>());
    }
    private static void require(boolean condition,String message) {
        if(!condition) throw new IllegalArgumentException(message);
    }
    private static void barrierViolation(String message) {
        barrierViol++; Util.log("OBS","VIOLATION_BARRIER",message);
    }
    private static void handle(Socket socket) {
        try(Socket s=socket;
            BufferedReader in=new BufferedReader(new InputStreamReader(s.getInputStream()));
            PrintWriter out=new PrintWriter(s.getOutputStream(),true)) {
            String line;
            while((line=in.readLine())!=null) {
                boolean finish=false;
                synchronized(L) {
                    if(finishing) { out.println("ERROR finalizando"); continue; }
                    try { process(line); out.println("OK"); }
                    catch(RuntimeException e) { protocolViol++; Util.log("OBS","VIOLATION_PROTOCOL",e.getMessage()+" event="+line); out.println("ERROR "+e.getMessage()); }
                    if(done.equals(nodes)) { finishing=true; finish=true; }
                }
                if(finish) finish();
            }
        } catch(IOException e) { Util.log("OBS","CONNECTION_CLOSED",e.toString()); }
    }
    private static void process(String line) {
        String[] p=line.trim().split("\\s+");
        require(p.length>=2,"Evento incompleto");
        String event=p[0], node=p[1];
        require(nodes.contains(node),"Processo desconhecido");
        require(!done.contains(node),"Evento apos DONE ou DONE duplicado");
        switch(event) {
            case "BEGIN" -> {
                require(p.length==5,"BEGIN invalido");
                String req=p[2]; int round=Integer.parseInt(p[3]); long wait=Long.parseLong(p[4]);
                require(round>0 && wait>=0,"Rodada ou espera invalida");
                require(req.matches("[A-Za-z0-9_]+-[0-9]+"),"request_id invalido");
                require(!active.containsKey(req)&&!completed.contains(req),"Pedido duplicado");
                require(active.values().stream().noneMatch(a->a.node().equals(node)),"Processo ja ativo");
                require(round==latest.getOrDefault(node,0)+1,"Rodada fora de sequencia");
                if(barrierMode) {
                    require(group(passed,round).contains(node),"BEGIN sem PASS");
                    if(group(arrivals,round).size()!=n) barrierViolation("BEGIN antes de todos chegarem");
                }
                latest.put(node,round); active.put(req,new Access(node,round)); waits.add(wait);
                if(first<0) first=System.nanoTime();
                maxActive=Math.max(maxActive,active.size());
                if(active.size()>1) { overlaps++; Util.log("OBS","VIOLATION_OVERLAP","request_id="+req+" active="+active.size()); }
                Util.log("OBS","BEGIN","node="+node+" request_id="+req+" active="+active.size());
            }
            case "END" -> {
                require(p.length==4,"END invalido"); String req=p[2]; int round=Integer.parseInt(p[3]);
                require(new Access(node,round).equals(active.get(req)),"END sem BEGIN correspondente");
                active.remove(req); completed.add(req); group(ended,round).add(node); last=System.nanoTime();
                perClient.merge(req.substring(0,req.lastIndexOf('-')),1,Integer::sum);
                Util.log("OBS","END","node="+node+" request_id="+req+" active="+active.size());
            }
            case "ARRIVE", "PASS", "LEAVE_READY", "LEFT" -> {
                require(barrierMode && p.length==3,"Evento de barreira invalido"); int r=Integer.parseInt(p[2]);
                require(r>0,"Rodada invalida");
                if(event.equals("ARRIVE")) {
                    require(r==latest.getOrDefault(node,0)+1,"ARRIVE fora de sequencia");
                    require(!group(arrivals,r).contains(node),"ARRIVE duplicado");
                    if(r>1 && (!group(left,r-1).contains(node)||group(ready,r-1).size()!=n)) barrierViolation("Avanco prematuro de rodada");
                    group(arrivals,r).add(node);
                } else if(event.equals("PASS")) {
                    require(group(arrivals,r).contains(node)&&!group(passed,r).contains(node),"PASS sem ARRIVE ou duplicado");
                    if(group(arrivals,r).size()!=n) barrierViolation("PASS antes de todos chegarem");
                    group(passed,r).add(node);
                } else if(event.equals("LEAVE_READY")) {
                    require(group(ended,r).contains(node)&&!group(ready,r).contains(node),"LEAVE_READY sem END ou duplicado");
                    group(ready,r).add(node);
                } else {
                    require(group(ready,r).contains(node)&&!group(left,r).contains(node),"LEFT sem prontidao ou duplicado");
                    if(group(ready,r).size()!=n) barrierViolation("LEFT antes de todos terminarem");
                    group(left,r).add(node);
                }
                Util.log("OBS",event,"node="+node+" round="+r);
            }
            case "DONE" -> {
                require(p.length==2,"DONE invalido");
                require(active.values().stream().noneMatch(a->a.node().equals(node)),"DONE com pedido ativo");
                if(barrierMode) {
                    int lastRound=latest.getOrDefault(node,0);
                    require(lastRound>0&&group(left,lastRound).contains(node),"DONE antes da saida da ultima barreira");
                }
                done.add(node);
            }
            default -> throw new IllegalArgumentException("Evento desconhecido");
        }
    }
    private static void finish() {
        int code; String row;
        synchronized(L) {
            Collections.sort(waits); int count=waits.size();
            double median=count==0?0:count%2==1?waits.get(count/2):(waits.get(count/2-1)/2.0+waits.get(count/2)/2.0);
            double p95=count==0?0:waits.get(Math.max(0,(int)Math.ceil(count*.95)-1));
            double total=first<0||last<0?0:(last-first)/1e9;
            boolean incomplete=completed.size()!=expected||!active.isEmpty();
            if(barrierMode) for(int r:arrivals.keySet())
                if(group(arrivals,r).size()!=n||group(passed,r).size()!=n||group(ended,r).size()!=n||group(ready,r).size()!=n||group(left,r).size()!=n) incomplete=true;
            code=overlaps+barrierViol+protocolViol>0?2:incomplete?3:0;
            String status=code==0?"OK":code==2?"VIOLACAO":"INCOMPLETO";
            StringJoiner pc=new StringJoiner(";"); perClient.forEach((k,v)->pc.add(k+"="+v));
            Util.log("OBS","SUMMARY",String.format(Locale.ROOT,
                "status=%s completed=%d expected=%d total_s=%.3f median_wait_ms=%.3f p95_wait_ms=%.3f max_active=%d overlaps=%d barrier_violations=%d protocol_violations=%d per_client=%s",
                status,completed.size(),expected,total,median,p95,maxActive,overlaps,barrierViol,protocolViol,pc));
            row=String.format(Locale.ROOT,"%s,%s,%s,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d,%s,%s%n",
                runId,label,seed,n,expected,completed.size(),total,median,p95,total>0?completed.size()/total:0,maxActive,overlaps,barrierViol,protocolViol,status,pc);
        }
        try {
            File f=new File(csv); if(f.getParentFile()!=null) f.getParentFile().mkdirs();
            boolean header=!f.exists()||f.length()==0;
            try(FileWriter w=new FileWriter(f,true)) {
                if(header) w.write("run_id,scenario,seed,nodes,expected,completed,total_s,median_wait_ms,p95_wait_ms,throughput_per_s,max_active,overlaps,barrier_violations,protocol_violations,status,per_client\n");
                w.write(row);
            }
        } catch(IOException e) { Util.log("OBS","CSV_ERROR",e.toString()); code=4; }
        System.exit(code);
    }
}
