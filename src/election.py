from os import getenv
import requests
import logging
import threading
import time
from fastapi import status

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] Nodo %(message)s")
logger = logging.getLogger(__name__)


NODE_ID = int(getenv("NODE_ID", 1))
PEERS = [p.strip() for p in getenv("PEERS").split(",") if p.strip()]


current_leader = None
is_election_in_progress = False
heartbeat_timeout_count = 0

HTTP_TIMEOUT = 1.5
HEARTBEAT_INTERVAL = 3.0
HEARTBEAT_THRESHOLD = 2

    
def get_id(url: str):
    """
    Extract Id of node from URL
    """
    try:
        hostname = url.split("//")[1].split(":")[0]
        return int(''.join(filter(str.isdigit, hostname)))
    except Exception:
        return 0

def _send_elec_rq(peer: str, result: list):
    # Send election message with NODE_ID
    try:
        response = requests.post(
            url= f"{peer}/election",
            json= {"sender_id": NODE_ID},
            timeout=HTTP_TIMEOUT
        )
        if response.status_code == status.HTTP_200_OK:
            result.append(True)
    except requests.RequestException:
        pass

def _send_coord_rq(peer: str):
    """
    Send NODE_ID from leader
    """
    try:
        requests.post(
            url=f"{peer}/coordinator",
            json={"leader_id": NODE_ID},
            timeout=HTTP_TIMEOUT
        )
    except requests.RequestException:
        pass


def start_election():
    #Control exit PEERS and NODE_ID value is possitive
    if not PEERS:
        raise "Not list of peers load"
    if NODE_ID < 0:
        raise "Invalid NODE_ID, value greater than 0"
    global is_election_in_progress, current_leader
    if is_election_in_progress:
        return
    is_election_in_progress = True
    logger.info(f"{NODE_ID}: Start election")

    higher_peers = [p for p in PEERS if get_id(p) > NODE_ID]
    if not higher_peers:
        declare_victory()
        return
    
    threads =[]
    results = []

    for peer in higher_peers:
        t = threading.Thread(target=_send_elec_rq, args=(peer, results))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    if not results:
        logger.info(f"{NODE_ID}: Any node greater response. Declare victory.")
        declare_victory()
    else:
        logger.info(f"{NODE_ID}: Waiting a new coodinator...")
        time.sleep(4.0)
        is_election_in_progress = False



def declare_victory():
    global current_leader, is_election_in_progress
    current_leader = NODE_ID
    is_election_in_progress = False
    logger.info(f"{NODE_ID}: Announce new leader")
    
    for peer in PEERS:
        t = threading.Thread(target=_send_coord_rq,args=(peer,))
        t.start()

def handle_election_message(sender_id: int) -> bool:
    """Respond to election from lower-ID node."""
    logger.info(f"{NODE_ID}: Analize ELECTION message recive from Node {sender_id}")
    
    if sender_id < NODE_ID:
        # Si el emisor es menor, se lanza una elección propia en paralelo 
        # y se retorna True para notificar la recepción exitosa del mensaje.
        threading.Thread(target=start_election).start()
        return True
        
    return False

def heartbeat_check():
    """Bucle continuo de monitoreo. Debe ejecutarse en su propio hilo dedicado."""
    global heartbeat_timeout_count, current_leader
    time.sleep(2.0) # Estabilización inicial
    
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        
        if current_leader == NODE_ID or is_election_in_progress:
            continue
         
        if current_leader is None:
            # Lanzar elección en un hilo separado para no bloquear este bucle de monitoreo
            threading.Thread(target=start_election).start()
            continue
                
        leader_url = next((p for p in PEERS if get_id(p) == current_leader), None)
        
        if not leader_url:
            threading.Thread(target=start_election).start()
            continue
            
        try:
            res = requests.get(f"{leader_url}/heartbeat", timeout=HTTP_TIMEOUT)
            if res.status_code == 200:
                heartbeat_timeout_count = 0
            else:
                heartbeat_timeout_count += 1
        except requests.RequestException:
            heartbeat_timeout_count += 1
            logger.warning(f"{NODE_ID}: Fail Heartbeat to leader {current_leader} ({heartbeat_timeout_count}/{HEARTBEAT_THRESHOLD})")
            
        if heartbeat_timeout_count >= HEARTBEAT_THRESHOLD:
            logger.error(f"{NODE_ID}: Leader {current_leader} DOWN.")
            heartbeat_timeout_count = 0
            current_leader = None
            threading.Thread(target=start_election).start()
    