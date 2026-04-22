import json
import os
import argparse

def generate_html_diagram(json_path, output_html):
    if not os.path.exists(json_path):
        print(f"Error: Could not find {json_path}")
        return

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    nodes = []
    edges = []

    for model in data.get('Models', []):
        node_id = model.get('Id')
        node_type = model.get('KSpiceType')
        
        # Color code based on type
        color = "#97C2FC" # Default blue
        if "ASC" in node_type or "Controller" in node_type or "PID" in node_type:
            color = "#FB7E81" # Red for controllers
        elif "Valve" in node_type:
            color = "#7BE141" # Green for valves
            
        nodes.append({
            "id": node_id,
            "label": f"{node_id}\n({node_type})",
            "color": color,
            "shape": "box"
        })

        for inp in model.get('Inputs', []):
            source_raw = inp.get('Source', '')
            dest = inp.get('Destination', '')
            
            # Source in MDL is usually formatted as "BlockName:Variable"
            source_block = source_raw.split(':')[0] if ':' in source_raw else source_raw
            
            if source_block:
                edges.append({
                    "from": source_block,
                    "to": node_id,
                    "label": f"{source_raw} -> {dest}",
                    "arrows": "to",
                    "font": {"align": "middle", "size": 10}
                })

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>K-Spice Topology Viewer</title>
        <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
        <style type="text/css">
            #mynetwork {{
                width: 100vw;
                height: 100vh;
                border: 1px solid lightgray;
                background-color: #f9f9f9;
            }}
            body {{
                margin: 0;
                padding: 0;
                overflow: hidden;
                font-family: sans-serif;
            }}
        </style>
    </head>
    <body>
    <div id="mynetwork"></div>
    <script type="text/javascript">
        var nodes = new vis.DataSet({json.dumps(nodes)});
        var edges = new vis.DataSet({json.dumps(edges)});

        var container = document.getElementById('mynetwork');
        var data = {{
            nodes: nodes,
            edges: edges
        }};
        var options = {{
            physics: {{
                stabilization: true,
                barnesHut: {{
                    springLength: 200,
                    springConstant: 0.04
                }}
            }},
            edges: {{
                smooth: {{
                    type: 'cubicBezier',
                    forceDirection: 'horizontal',
                    roundness: 0.4
                }}
            }},
            layout: {{
                hierarchical: {{
                    direction: 'LR',
                    sortMethod: 'directed'
                }}
            }}
        }};
        var network = new vis.Network(container, data, options);
    </script>
    </body>
    </html>
    """

    os.makedirs(os.path.dirname(output_html), exist_ok=True)
    with open(output_html, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"Topology Diagram rendered successfully: {os.path.abspath(output_html)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", required=True, help="Path to KSpiceSystemMap.json")
    parser.add_argument("--out", required=True, help="Path to output html")
    args = parser.parse_args()
    
    generate_html_diagram(args.json, args.out)
