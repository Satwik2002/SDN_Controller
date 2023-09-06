from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ipv4
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
from ryu.lib.packet import arp

from ryu.topology import event, switches
from ryu.topology.api import get_switch, get_link, get_all_host, get_host
import time

from ryu.app.simple_monitor_13 import *

from ryu.lib import hub

import json
import sys

from ryu.app import simple_switch_13
from webob import Response
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.app.wsgi import ControllerBase, WSGIApplication, route
from ryu.lib import dpid as dpid_lib

simple_switch_instance_name = 'simple_switch_api_app'
url = '/simpleswitch/mactable'

try:
    f1 = open('info.txt', 'r')
    lines = f1.readlines()

except Exception as e:
    print('Error:', e)
    exit()

# Flow : Forwarding entry
class SimpleSwitch13(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(SimpleSwitch13, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.topology_api_app = self
        self.dm_links = {}
        self.hs_links = {}
        self.h = []
        self.links = {}
        self.switch_list = []
        self.switches = []
        self.links_list = []
        self.n_hosts = 0
        self.datapaths = {}
        self.flows = {}

        self.num_s = int(lines[0].split()[0])
        self.num_h = int(lines[0].split()[1])

        for i in range(1, self.num_s + 1):
            self.flows[i] = []
        
        self.pathinfo = [[{'cost': 0, 'dl': 0, 'bw': 0}] * self.num_s for _ in range(self.num_s)]
        self.adj = [[0]*self.num_s for _ in range(self.num_s)] # contains all costs

        # Initialize costs
        for i in range(1,len(lines)):
            l = lines[i].split()
            if l[0][0] == 'S' and l[1][0] == 'S':
                a, b = int(l[0][1:]) - 1, int(l[1][1:]) - 1
                dl, bw = int(l[3]), int(l[2])
                self.pathinfo[a][b] = {'cost': dl/bw, 'dl': dl, 'bw': bw}
                self.pathinfo[b][a] = {'cost': dl/bw, 'dl': dl, 'bw': bw}

                self.adj[a][b] = dl/bw
                self.adj[b][a] = dl/bw

        print("Link costs...")

        # Print costs
        for i in range(len(self.adj)):
            for j in range(len(self.adj[i])):
                if i!=j:
                    if self.adj[i][j]==0:
                        print("switch",i,"- switch",j,": ",-1)
                    else:
                        print("switch",i,"- switch",j,": ",self.adj[i][j])

        wsgi = kwargs['wsgi']
        wsgi.register(SimpleSwitchController,
                      {simple_switch_instance_name: self})

# Get switch and link information
    @set_ev_cls(event.EventSwitchEnter)
    def get_topology_data(self, ev):
        self.switch_list = get_switch(self.topology_api_app, None)
        self.switches = [switch.dp.id for switch in self.switch_list]
        self.links_list = get_link(self.topology_api_app, None)
        for link in self.links_list:
            self.links[(link.src.dpid, link.dst.dpid)] = link.src.port_no
        print ("switches ", self.switches)
        print ("links ", self.links)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        self.datapaths[datapath.id] = datapath
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)


    # Function to all flow to the switches
    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    # Packet in event handler
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              ev.msg.msg_len, ev.msg.total_len)
        msg = ev.msg
        datapath = msg.datapath
        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            #ignore lldp packet
            return
        dst = eth.dst
        src = eth.src
	
        pkt_arp = pkt.get_protocol(arp.arp)
        if pkt_arp:
            # Destination and source ip address
            d_ip = pkt_arp.dst_ip
            s_ip = pkt_arp.src_ip

            # Destination and source mac address (HW address)
            d_mac = pkt_arp.dst_mac
            s_mac = pkt_arp.src_mac
            
            if (s_ip, s_mac) not in self.h:
                self.hs_links[s_ip] = [dpid, in_port]
                self.hs_links[s_mac] = [dpid, in_port]
                self.h.append((s_ip, s_mac))
                self.dm_links[dpid] = s_mac
                # Prints hosts and their connections
                print("hosts: ", self.h)
                print("host connections: ", self.hs_links)  
            
            
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                return
            # else:
                # self.add_flow(datapath, 2, match, actions)
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)
    
    # Algorithm to find the shortest path between two nodes of a graph
    def dijkstra(self, start_vertex, end_vertex):
        adjacency_matrix = self.adj
        NO_PARENT = -1
        n_vertices = len(adjacency_matrix[0])
        min_distances = [sys.maxsize] * n_vertices

        visited = [False] * n_vertices

        for vertex_index in range(n_vertices):
            min_distances[vertex_index] = sys.maxsize
            visited[vertex_index] = False
            
        min_distances[start_vertex] = 0

        parents = [-1] * n_vertices
        parents[start_vertex] = NO_PARENT

        for i in range(1, n_vertices):
            nearest_vertex = -1
            shortest_distance = sys.maxsize
            for vertex_index in range(n_vertices):
                if not visited[vertex_index] and min_distances[vertex_index] < shortest_distance:
                    nearest_vertex = vertex_index
                    shortest_distance = min_distances[vertex_index]
            visited[nearest_vertex] = True

            for vertex_index in range(n_vertices):
                edge_distance = adjacency_matrix[nearest_vertex][vertex_index]
                
                if edge_distance > 0 and shortest_distance + edge_distance < min_distances[vertex_index]:
                    parents[vertex_index] = nearest_vertex
                    min_distances[vertex_index] = shortest_distance + edge_distance

        path = []
        current_vertex = end_vertex
        while current_vertex != NO_PARENT:
            path.append(current_vertex+1)
            current_vertex = parents[current_vertex]

        path.reverse()
        return path
    
    def calculate_shortest_path(self, src, dst, bw):
        path = self.dijkstra(src-1, dst-1)
        for i in range(len(path)-1):
            if self.pathinfo[path[i]-1][path[i+1]-1]['bw'] < bw:
                return -1
        return path

    # Code to install path as per shortest path
    def install_path(self, s, d, type, bw_usr):
        h = [s, d]
        for src in h:
            for dst in h:
                if src != dst:
                    path = self.calculate_shortest_path(self.hs_links[src][0], self.hs_links[dst][0], bw_usr)
                    print(path)
                    if path == -1:
                        return -1
                    for i in range(0, len(path)):
                        switch = path[i]
                        if i != len(path) - 1 and i != 0:
                            next_hop = path[i+1]
                            prev_hop = path[i-1]
                            self.pathinfo[switch-1][next_hop-1]['bw'] -= bw_usr
                            self.adj[switch-1][next_hop-1] = (self.pathinfo[switch-1][next_hop-1]['dl'])/(self.pathinfo[switch-1][next_hop-1]['bw'])
                            self.pathinfo[switch-1][next_hop-1]['cost'] = (self.pathinfo[switch-1][next_hop-1]['dl'])/(self.pathinfo[switch-1][next_hop-1]['bw'])
                            in_port = self.links[(switch, prev_hop)]
                            out_port = self.links[(switch, next_hop)]
                            self.pathinfo[switch-1][next_hop-1]
                        elif i == len(path) - 1:                          
                            prev_hop = path[i-1]
                            in_port = self.links[(switch, prev_hop)]
                            out_port = self.hs_links[dst][1]
                        else:                          
                            next_hop = path[i+1]
                            self.pathinfo[switch-1][next_hop-1]['bw'] -= bw_usr
                            self.adj[switch-1][next_hop-1] = (self.pathinfo[switch-1][next_hop-1]['dl'])/(self.pathinfo[switch-1][next_hop-1]['bw'])
                            self.pathinfo[switch-1][next_hop-1]['cost'] = (self.pathinfo[switch-1][next_hop-1]['dl'])/(self.pathinfo[switch-1][next_hop-1]['bw'])                          
                            in_port = self.hs_links[src][1]
                            out_port = self.links[(switch, next_hop)]
                        
                        datapath = self.datapaths[switch]
                        parser = datapath.ofproto_parser
                        actions = [parser.OFPActionOutput(out_port)]

                        if type == 'ip':
                            match = parser.OFPMatch(in_port=in_port, eth_type=0x0800, ipv4_src=src, ipv4_dst=dst)
                        elif type == 'mac':
                            match = parser.OFPMatch(in_port=in_port, eth_type=0x0800, eth_src=src, eth_dst=dst)
                        self.flows[switch].append((in_port, src, dst, out_port))
                        self.add_flow(datapath, 1, match, actions)
        return 1


# REST API for the controller
class SimpleSwitchController(ControllerBase): 
    def __init__(self, req, link, data, **config):
        super(SimpleSwitchController, self).__init__(req, link, data, **config)
        self.simple_switch_app = data[simple_switch_instance_name]

    @route('simpleswitch', url, methods=['PUT'])
    def put_mac_table(self, req, **kwargs):
        simple_switch = self.simple_switch_app
        try:
            new_entry = req.json if req.body else {}
        except ValueError:
            raise Response(status=400)
	
        if 'dst' in new_entry:
            try:
                res = simple_switch.install_path(new_entry['src'], new_entry['dst'], new_entry['type'], int(new_entry['bw']))
                if res == -1:
                    return Response(body="Not enough bandwidth\n")
                else:
                    return Response(body="Done\n")
            except Exception as e:
                print(e)
                return Response(status=500)
        elif 'src' in new_entry:
            try:
                ans = []
                for i in range(1,len(simple_switch.h)+1):
                    src = '10.0.0.' + str(new_entry['src'])
                    dst = '10.0.0.' + str(i)
                    if src != dst:
                        res = simple_switch.dijkstra(simple_switch.hs_links[src][0]-1, simple_switch.hs_links[dst][0]-1)
                        # ans.append(res)
                        print("HSrc: ",src," Dst:",dst ," switches", res)

                if res == -1:
                    return Response(body="Not enough bandwidth\n")
                else:
                    return Response(body="Done\n")
            except Exception as e:
                print(e)
                return Response(status=500) 

        else:
            dp = int(new_entry['dpid'])
            print(simple_switch.flows[dp])
            return Response(body = "Done\n")