"""
Streamlit App: Multi-Provider Freight Route Finder
===================================================
Interactive UI for the bidirectional A* pathfinding engine.

Features:
- Route finder (cost, time, multi-criteria optimization)
- Zone/provider analytics
- Graph visualization
- Data source configuration
"""

import streamlit as st
from datetime import datetime
import json
from data_loader import TerminusDBLoader
from engine import BidirectionalAStarEngine
from engine_new import FreightAStarEngine, RouteOptimizer
from data_model import Shipment

# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="Freight Route Finder",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📦 Multi-Provider Freight Route Finder")
st.markdown("**Optimized routing across independent provider networks using bidirectional A***")

# ============================================================================
# SIDEBAR: SETTINGS & DATA SOURCE
# ============================================================================

with st.sidebar:
    st.header("⚙️ Settings")

    # Data source selection
    st.subheader("Data Source")
    data_source = st.radio(
        "Select data source",
        ["PoC (Synthetic)", "Production (RDBMS)"]
    )
    use_rdbms = data_source == "Production (RDBMS)"

    if use_rdbms:
        st.warning("⚠️ RDBMS mode requires DATABASE_URL environment variable")
    
    # Load data
    @st.cache_resource
    def load_data(_use_rdbms):
        loader = TerminusDBLoader(use_rdbms=_use_rdbms)

        if _use_rdbms:
            _, graph_index = loader.load_from_rdbms()
        else:
            _, _, _, _, graph_index = loader.load_sample_data()
            
        return graph_index
    
    try:
        graph_index = load_data(use_rdbms)
    
        postcodes_dict = graph_index.postcodes
        # Create engine and optimizer
        engine = FreightAStarEngine(graph_index, postcodes_dict)
        optimizer = RouteOptimizer(engine)
        
    except Exception as e:
        st.error(f"❌ Failed to load data: {e}")
        st.stop()
    
    # Search parameters
    st.subheader("Search Parameters")
    max_cost = st.number_input("Max cost (AUD)", value=1000, min_value=0, step=20)
    max_etd = st.number_input("Max ETD (hours)", value=96, min_value=0, step=1)
    max_hops = st.number_input("Max provider hops", value=3, min_value=1, max_value=7)
    
    # Export options
    st.subheader("Export")
    if st.button("📥 Export graph as JSON"):
        try:
            loader = TerminusDBLoader(use_rdbms=use_rdbms)
            loader.export_graph_json("graph_export.json")
            st.success("✅ Exported to graph_export.json")
        except Exception as e:
            st.error(f"❌ Export failed: {e}")
# ============================================================================
# TABS: HOME | ROUTE FINDER | ANALYTICS
# ============================================================================

tabs = st.tabs(["🏠 Home", "🔍 Route Finder", "📊 Analytics"]) 

# ============================================================================
# TAB 1: HOME
# ============================================================================

with tabs[0]:
    st.header("System Overview")

    col1, col2 = st.columns(2)

    with col1:
        st.metric("Providers", len(graph_index.providerZones))
        st.metric("Total Zones", sum(len(z) for z in graph_index.providerZones.values()))
    
    with col2:
        st.metric("Postcodes", len(graph_index.postcodes))
        st.metric("Zone Routes", sum(len(r) for r in graph_index.zoneRoutes.values()))

    st.markdown("---")

    st.subheader("Architecture")
    st.markdown("""
    ### Multi-Layer Graph Structure
    
    **Layer 1: Per-Provider Zone Graphs**
    - Isolated zone networks per freight provider
    - No global zone reconciliation (avoids overlapping definition conflicts)
    - Each provider maintains its own pricing and routing rules
    
    **Layer 2: Shared Postcode Layer**
    - Universal postcode nodes across all providers
    - Entry/exit points for provider network handoffs
    - No dense postcode-to-postcode edges (avoids combinatorial explosion)
    
    ### Bidirectional A* Algorithm
    
    **Why Bidirectional?**
    - Forward search: origin → intermediate postcodes
    - Backward search: destination → intermediate postcodes
    - Meet in middle: dramatically reduces search space
    - Complexity: O(√N) per direction vs O(N) unidirectional
    **Pruning Strategy**
    - Cost threshold: eliminate suboptimal paths early
    - ETD threshold: respect time constraints
    - Max hops: limit provider transitions
    - Reliability filter: prefer trusted providers
    
    **Performance**
    - ~140K edges in graph (per-provider zones + postcode links)
    - Typical query: <1 second for Sydney→Melbourne
    - Scales to 100+ providers without explosion
    """)

    st.markdown("---")

    st.subheader("Provider Networks")
    for provider_id, zones in graph_index.providerZones.items():
        with st.expander(f"**{provider_id}** ({len(zones)} zones)"):
            for zone in zones:
                postcodes_str = ", ".join(zone.postcodes[:5])
                if len(zone.postcodes) > 5:
                    postcodes_str += f", + {len(zone.postcodes) - 5} more"
                st.text(f"• {zone.zoneCode:15} | {postcodes_str}") 
# ============================================================================
# TAB 2: ROUTE FINDER
# ============================================================================

with tabs[1]:
    st.header("🔍 Find Best Route(s)")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Shipment Details")
        origin_pc = st.selectbox("Origin Postcode", sorted(graph_index.postcodes.keys()), key="origin")
        weight_kg = st.number_input("Weight (kg)", value = 100, min_value=1, step=10)
    
    with col2:
        st.subheader("")
        dest_pc = st.selectbox("Destination Postcode", sorted(graph_index.postcodes.keys()), key="dest")
        volume_cbm = st.number_input("Volume (CBM)", value=0.5, min_value=0.0, step=0.1)

    # Optimization criteria
    st.markdown("---")
    criteria = st.radio(
        "Optimization Criteria",
        ["None", "Lowest Cost", "Fastest", "Bit of Both"],
        horizontal=True
    )

    # Find Routes
    if st.button("🚀 Find Routes", key="find_routes"):
        if not origin_pc or not dest_pc:
            st.error("❌ Please select both origin and destination postcodes")
        else:
            shipment = Shipment(
                originPC=origin_pc,
                originSbrb=graph_index.postcodes[origin_pc].suburb,
                originState=graph_index.postcodes[origin_pc].state,
                destPC=dest_pc,
                destSbrb=graph_index.postcodes[dest_pc].suburb,
                destState=graph_index.postcodes[dest_pc].state,
                weightKG=weight_kg,
                volumeCBM=volume_cbm
            )
        
        try:
            with st.spinner("🔍 Searching for optimal routes..."):
                if criteria == "None":
                    paths = optimizer.unoptimized(shipment)
                elif criteria == "Lowest Cost":
                    paths = optimizer.optimized_for_cost(shipment, maxETD=max_etd)
                elif criteria == "Fastest":
                    paths = optimizer.optimized_for_time(shipment, maxCost=max_cost)
                else:
                    paths = optimizer.optimize_multi_criteria(shipment)

            st.markdown("---")
            if isinstance(paths, list):
                num_paths = len(paths)
            elif paths is not None:
                paths = [paths]
                num_paths = 1
            else:
                paths = []
                num_paths = 0
            st.subheader(f"Top Routes (0-{num_paths})")

            for i, path in enumerate(paths, 1):
                with st.expander(f"**Route {i}** | Cost: ${path.totalCost:.2f} | ETD: {path.totalETD:.1f} hrs | Providers: {', '.join(path.providersInvolved)}"):
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric("Total Cost", f"${path.totalCost:.2f}")
                    with col2:
                        st.metric("ETD", f"{path.totalETD:.1f} hrs")
                    with col3:
                        st.metric("Reliability", f"{path.reliabilityScore:.2%}")
                    
                    st.markdown("**Path Segments:**")
                    for j, segment in enumerate(path.segments, 1):
                        provider = segment.get('providerId', 'Unknown')
                        from_zone = segment.get('fromZone', '-')
                        to_zone = segment.get('toZone', '-')
                        cost = segment.get('cost', 0)
                        etd = segment.get('etd', 0)
                        st.text(f" {j}. {provider}: {from_zone} → {to_zone} | ${cost:.2f} | {etd:.1f} hrs")
                    
                    # Export Route
                    if st.button(f"📥 Export Route {i}", key=f"export_{i}"):
                        st.json(vars(path))
        except Exception as e:
            st.error(f"❌ Route search failed: {e}")
            st.exception(e)
# ============================================================================
# TAB 3: ANALYTICS
# ============================================================================

with tabs[2]:
    st.header("📊 Network Analytics")

    analytics_type = st.radio(
        "Select Analysis",
        ["Zone Coverage", "Provider Comparision", "Postcode Distribution"],
        horizontal=True
    )

    if analytics_type == "Zone Coverage":
        st.subheader("Zone Coverage by Provider")

        for provider_id, zones in graph_index.providerZones.items():
            total_pcs = sum(len(z.postcodes) for z in zones)
            st.markdown(f"### {provider_id} ({len(zones)} zones, {total_pcs} postcodes)")

            zone_data = []
            for zone in zones:
                zone_data.append({
                    'Zone': zone.zoneCode,
                    'Postcodes': len(zone.postcodes),
                    'State': zone.state,
                    'Category': zone.category
                })
            
            st.dataframe(zone_data, use_container_width=True)

    elif analytics_type == "Provider Comparision":
        st.subheader("Provider Network Metrics")

        comparison = []
        for provider_id, zones in graph_index.providerZones.items():
            routes = graph_index.zoneRoutes.get(provider_id, [])
            comparison.append({
                'Provider': provider_id,
                'Zones': len(zones),
                'Routes': len(routes),
                'Avg Reliability': sum(r.reliabilityScore for r in routes) / len(routes) if routes else 0,
                'Min Charge': min((r.minCharge for r in routes), default=0),
                'Avg Base Charge': sum(r.baseCharge for r in routes) / len(routes) if routes else 0
            })
        
        st.dataframe(comparison, use_container_width=True)
    
    else:
        st.subheader("Postcode Distribution by State")

        state_dist = {}
        for pc in graph_index.postcodes.values():
            state = pc.state
            if state not in state_dist:
                state_dist[state] = 0
            state_dist[state] += 1
        
        import pandas as pd
        df_state = pd.DataFrame(list(state_dist.items()), columns=['State', 'Count'])

        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(df_state, use_container_width=True)
        with col2:
            st.bar_chart(df_state.set_index('State'))
# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.caption(f"Freight Route Finder v1.4 | Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")