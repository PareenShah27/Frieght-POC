"""
Bidirectional A* Engine for Multi-Provider Zone Graph
======================================================
Finds optimal multi-provider paths using bidirectional search
without global zone reconciliation.

Algorithm:
1. Forward search from origin postcode
2. Backward search from destination postcode
3. Meet in middle at postcode handoff points
4. Reconstruct best path segments
"""

import heapq
import math
from typing import Dict, List, Tuple
from data_model import Postcode, Shipment, MultiHopPath, GraphIndex

class BidirectionalAStarEngine:
    """Multi-provider zone graph pathfinding with bidirectional A*"""

    def __init__(self, graph_index: GraphIndex, postcodes_dict: Dict[str, Postcode]):
        self.index = graph_index
        self.postcodes = postcodes_dict

        # Geo coordinates for heuristic (simplified: use state distance)
        self.state_coords = {
            'NSW': (150.9, -33.9),  # Sydney
            'VIC': (145.1, -37.8),  # Melbourne
            'QLD': (153.0, -27.5),  # Brisbane
            'WA': (115.9, -31.9),   # Perth
            'SA': (139.2, -34.4),   # Adelaide
            'TAS': (147.1, -42.9),  # Hobart
            'ACT': (149.2, -35.3),  # Canberra
            'NT': (130.8, -12.5),   # Darwin
        }
    
    def find_mltihop_path(self, shipment: Shipment, maxCost: float = float('inf'), maxETD: float = float('inf'), maxHops: int = 5, topK: int = 10) -> List[MultiHopPath]:
        """
        Find best multi-provider paths using bidirectional A*
        
        Args:
            shipment: Shipment with origin/dest postcodes and weight
            max_cost: Cost threshold for pruning
            max_etd: ETD threshold for pruning
            max_hops: Maximum provider transitions
            top_k: Return top K paths
        
        Returns:
            List of MultiHopPath sorted by cost (ascending)
        """

        if shipment.originPC not in self.postcodes:
            raise ValueError(f"Origin postcode: {shipment.originPC} not found")

        if shipment.destPC not in self.postcodes:
            raise ValueError(f"Destination postcode: {shipment.destPC} not found")
        

        # Run bidirectional search
        forward_paths = self._astar_search(shipment, shipment.originPC, shipment.destPC, True, maxCost, maxETD, maxHops)
        backward_paths = self._astar_search(shipment, shipment.destPC, shipment.originPC, False, maxCost, maxETD, maxHops)

        #Merge and reconstruct paths
        all_paths = self._merge_paths(shipment, forward_paths, backward_paths)

        # Rank and return top K paths
        all_paths.sort(key=lambda p: p.totalCost)
        return all_paths[:topK]

    def _astar_search(self, shipment: Shipment, startPC: str, goalPC: str, forward: bool, maxCost: float, maxETD: float, maxHops: int) -> Dict:
        """
        Single-direction A* search
        
        Returns:
            Dictionary: node → (parent_node, cost, etd, hops, route_details)
        """
        
        # Priority queue: (f_cost, counter, node)
        counter = 0
        open_set = []

        start_node = ('pc', startPC, None)
        start_cost = 0.0
        start_h = self._heuristic(startPC, goalPC)

        heapq.heappush(open_set, (start_h, counter, start_node))
        counter += 1

        # State tracking
        g_score = {start_node: start_cost} # actual cost
        f_score = {start_node: start_h}    # estimated total cost
        came_from = {}   # for path reconstruction
        edge_data = {}   #  store edge details (route, etc.)

        visited = set()

        while open_set:
            current_f, _, current = heapq.heappop(open_set)

            if current in visited:
                continue
            visited.add(current)

            current_g = g_score[current]
            node_type, node_id, provider_id = current
            
            # Pruning: cost/etd thresholds
            if current_g > maxCost:
                continue
                
            # Get neighbors based on node type
            neighbors = self._get_neighbors(current, shipment, maxHops, forward)

            for neighbor, edge_cost, edge_etd, route_info in neighbors:
                neighbor_g = current_g + edge_cost
                neighbor_h = self._heuristic_node(neighbor, goalPC)
                neighbor_f = neighbor_g + neighbor_h

                # Skip if this path is suboptimal
                if neighbor in g_score and neighbor_g >= g_score[neighbor]:
                    continue
                
                # Skip if exceeds thresholds
                if neighbor_g > maxCost or edge_etd > maxETD:
                    continue
                
                came_from[neighbor] = current
                g_score[neighbor] = neighbor_g
                f_score[neighbor] = neighbor_f
                edge_data[neighbor] = {
                    'cost': edge_cost,
                    'etd': edge_etd,
                    'route': route_info
                }

                heapq.heappush(open_set, (neighbor_f, counter, neighbor))
                counter += 1
        
        return {
            'parent': came_from,
            'cost': g_score,
            'edge': edge_data
        }

    def _get_neighbors(self, node: Tuple, shipment: Shipment, maxHops: int, forward: bool = True) -> List[Tuple[Tuple, float, float, Dict]]:
        """
        Get neighboring nodes from current node
        
        Returns:
            List of (neighbor_node, edge_cost, edge_etd, route_info)
        """

        node_type, node_id, provider_id = node
        neighbors = []

        if node_type == 'pc':
            # Postcode → ProviderZone (enter provider network)
            for p_id, zone_code in self.index.get_ZonesForPostcode(node_id):
                neighbor = ('pz', zone_code, p_id)
                # Zero cost to enter zone
                neighbors.append((neighbor, 0.0, 0.0, {'type': 'entry' if forward else 'exit'}))
        
        elif node_type == 'pz':
            if forward:
                # Forward: Zone -> Outgoing Routes -> Next Zone
                routes = self.index.get_OutgoingRoutes(provider_id, node_id)
                for route in routes:
                    neighbor = ('pz', route.toZone, provider_id)
                    edge_cost = route.calculateCost(shipment.weightKG)
                    neighbors.append((neighbor, edge_cost, route.deliveryHrs, {
                        'type': 'zone_route',
                        'service': route.serviceType,
                        'route': route
                    }))
            else:
                # Backward: Zone -> Incoming Routes -> Previous Zone
                routes = self.index.get_IncomingRoutes(provider_id, node_id)
                for route in routes:
                    neighbor = ('pz', route.fromZone, provider_id)
                    edge_cost = route.calculateCost(shipment.weightKG)
                    neighbors.append((neighbor, edge_cost, route.deliveryHrs, {
                        'type': 'zone_route',
                        'service': route.serviceType,
                        'route': route
                    }))
            
            # Zone → Postcode (exit to transfer point)
            pcs = self.index.get_PostcodesForZone(provider_id, node_id)
            for pc in pcs:
                neighbor = ('pc', pc, None)
                # Zero cost to exit (transfer cost handled separately if needed)
                neighbors.append((neighbor, 0.0, 0.0, {'type': 'exit' if forward else 'entry'}))
        
        return neighbors
    
    def _heuristic(self, pcA: str, pcB: str) -> float:
        """
        Admissible heuristic: geographic distance between postcodes
        Simplified: use state-level distance
        """

        pcA_obj = self.postcodes.get(pcA)
        pcB_obj = self.postcodes.get(pcB)

        if not pcA_obj or not pcB_obj:
            return 0.0
        
        stateA = pcA_obj.state or 'NSW'
        stateB = pcB_obj.state or 'NSW'

        if stateA not in self.state_coords or stateB not in self.state_coords:
            return 0.0
        
        lonA, latA = self.state_coords[stateA]
        lonB, latB = self.state_coords[stateB]

        # Euclidean distance scaled by typical $/km (very rough estimate)
        distance = math.sqrt((lonA - lonB)**2 + (latA - latB)**2)
        return distance * 0.01
    
    def _heuristic_node(self, node: Tuple, goalPC: str) -> float:
        """Heuristic from any node to goal"""
        node_type, node_id, provider_id = node

        if node_type == 'pc':
            return self._heuristic(node_id, goalPC)
        else:
            pcs = self.index.get_PostcodesForZone(provider_id, node_id)
            if pcs:
                return self._heuristic(pcs[0], goalPC)
        return 0.0

    def _reconstruct_path(self, goal: Tuple, came_from: Dict, g_score: Dict, edge_data: Dict, final_cost: float) -> Dict:
        """Reconstruct path from start to goal"""

        path = {}
        current = goal

        while current in came_from:
            parent = came_from[current]
            path[current] = {
                'parent': parent,
                'cost': g_score[current],
                'edge': edge_data.get(current, {})
            }
            current = parent
        
        path[current] = {'parent': None, 'cost': 0.0, 'edge': {}}
        return path
    
    def _unroll_path(self, end_node: Tuple, came_from: Dict, edge_data: Dict) -> Tuple[List[Tuple], List[Dict], float, float]:
        """
        Backtracks from end_node to start using came_from pointers.
        Returns (nodes_list, segments_list, total_cost, total_etd)
        """

        nodes = []
        segments = []
        totalCost = 0.0
        totalETD = 0.0

        curr = end_node
        while curr is not None:
            nodes.append(curr)

            if curr in edge_data:
                data = edge_data[curr]
                totalCost += data['cost']
                totalETD += data['etd']

                route_info = data['route']
                seg = {
                    'fromZone': came_from[curr][1] if came_from[curr] else 'START',
                    'toZone': curr[1],
                    'cost': data['cost'],
                    'etd': data['etd'],
                    'type': route_info.get('type', ''),
                    'service': route_info.get('service', '')
                }

                if 'route' in route_info and hasattr(route_info['route'], 'providerId'):
                    seg['providerId'] = route_info['route'].providerId
            
                segments.append(seg)
            
            curr = came_from.get(curr)

        return nodes, segments, totalCost, totalETD
    
    def _merge_paths(self, shipment: Shipment, forward: Dict, backward: Dict) -> List[MultiHopPath]:
        """
        Merge forward and backward search results
        Find common postcodes where paths can meet
        """
        paths = []

        fwd_visited = forward['cost']
        bwd_visited = backward['cost']

        common_nodes = set(fwd_visited.keys()) & set(bwd_visited.keys())

        for meet_node in common_nodes:
            if meet_node[0] != 'pc':
                continue

            # 1. Trace Start -> Meet
            f_nodes, f_segs, f_cost, f_etd = self._unroll_path(meet_node, forward['parent'], forward['edge'])
            f_nodes.reverse()
            f_segs.reverse()

            # 2. Trace Meet -> End
            b_nodes, b_segs, b_cost, b_etd = self._unroll_path(meet_node, backward['parent'], backward['edge'])

            # 3. Combine
            full_nodes = f_nodes + b_nodes[1:]
            full_segs = f_segs + b_segs
            total_cost = f_cost + b_cost
            total_etd = f_etd + b_etd
            
            # Extract Providers
            providers = set()
            for seg in full_segs:
                if 'providerId' in seg:
                    providers.add(seg['providerId'])
            
            path = MultiHopPath(
                shipmentId=shipment.id,
                totalCost=total_cost,
                totalETD=total_etd,
                nodes=full_nodes,
                segments=full_segs,
                providersInvolved=list(providers),
                numHops=len(providers)
            )
            paths.append(path)

        if not paths:
            return [self.create_default_path(shipment)]

        return paths
    
    def create_default_path(self, shipment: Shipment) -> MultiHopPath:
        """Fallback single-hop path if bidirectional search fails"""
        return MultiHopPath(shipmentId=shipment.id, totalCost=float('inf'), totalETD=float('inf'), nodes=[('pc', shipment.originPC, None), ('pc', shipment.destPC, None)], numHops=0)
    
class RouteOptimizer:
    """High-level API for route optimization"""

    def __init__(self, engine: BidirectionalAStarEngine):
        self.engine = engine

    def unoptimized(self, shipment: Shipment) -> MultiHopPath:
        return self.engine.create_default_path(shipment)

    def optimized_for_cost(self, shipment: Shipment, maxETD: float = float('inf')) -> List[MultiHopPath]:
        """Get cheapest route(s)"""
        paths = self.engine.find_mltihop_path(shipment, maxETD=maxETD, topK=10)
        return paths
    
    def optimized_for_time(self, shipment: Shipment, maxCost: float = float('inf')) -> List[MultiHopPath]:
        """Get fastest route(s)"""
        paths = self.engine.find_mltihop_path(shipment, maxCost=maxCost, topK=10)
        paths.sort(key= lambda p: p.totalETD)
        return paths
    
    def optimize_multi_criteria(self, shipment: Shipment) -> List[MultiHopPath]:
        """Get balanced routes using cost+time+reliability"""
        paths = self.engine.find_mltihop_path(shipment, topK=15)

        # Simple TOPSIS-like scoring
        for i, path in enumerate(paths):
            # Normalize metrics
            costScore = 1.0 / (1.0 + path.totalCost / 1000.0) # Inverse
            timeScore = 1.0 / (1.0 + path.totalETD / 24.0) # Inverse
            reliabilityScore = path.reliabilityScore

            # Weighted Combination
            path.totScore = (0.4 * costScore + 0.35 * timeScore + 0.25 * reliabilityScore)
        
        paths.sort(key=lambda p: p.totScore, reverse=True)
        for i, path in enumerate(paths):
            path.rank = i + 1
        
        return paths