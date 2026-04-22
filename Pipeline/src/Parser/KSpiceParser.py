import os
import sys
import glob
import xml.etree.ElementTree as ET
import json
import argparse

class KSpiceParser:
    """
    Phase 1: Automated K-Spice XML Extractor
    Reads the .mdl and .prm files to extract components, topology, and physical parameters.
    Outputs a clean, agnostic intermediate JSON dictionary.
    """
    
    def __init__(self, mdl_path, prm_path):
        self.mdl_path = mdl_path
        self.prm_path = prm_path
        self.blocks = {}

    def parse(self):
        print(f"Parsing MDL: {self.mdl_path}")
        self._parse_mdl()
        
        if self.prm_path and os.path.exists(self.prm_path):
            print(f"Parsing PRM: {self.prm_path}")
            self._parse_prm()
            
        return self._build_tsa_dict()

    def _parse_mdl(self):
        tree = ET.parse(self.mdl_path)
        root = tree.getroot()
        
        for block in root.findall('.//Block'):
            name = block.get('Name')
            b_type = block.get('Type')
            
            if not name or not b_type:
                continue
                
            block_data = {
                'Name': name,
                'Type': b_type,
                'Inputs': [],
                'Parameters': {}
            }
            
            # Extract connections (Inputs)
            inputs = block.find('Inputs')
            if inputs is not None:
                for inp in inputs.findall('Input'):
                    src_node = inp.find('Source')
                    dest_node = inp.find('Destination')
                    
                    if src_node is not None and dest_node is not None:
                        block_data['Inputs'].append({
                            'Source': src_node.text,
                            'Destination': dest_node.text
                        })
            
            self.blocks[name] = block_data

    def _parse_prm(self):
        try:
            tree = ET.parse(self.prm_path)
            root = tree.getroot()
        except Exception as e:
            print(f"Error reading PRM: {e}")
            return
            
        for block in root.findall('.//Block'):
            name = block.get('Name')
            # The PRM block doesn't specify Type, but we can match it by Name from the MDL list
            if name not in self.blocks:
                continue
                
            data_items = block.find('DataItems')
            if data_items is not None:
                for item in data_items.findall('DataItem'):
                    item_name = item.get('Name')
                    
                    # Single value
                    val_node = item.find('Value')
                    if val_node is not None and val_node.text:
                        self.blocks[name]['Parameters'][item_name] = self._try_convert(val_node.text)
                        
                    # Array values
                    arr_node = item.find('Array')
                    if arr_node is not None:
                        arr_vals = []
                        for val_in_arr in arr_node.findall('Value'):
                            if val_in_arr.text:
                                arr_vals.append(self._try_convert(val_in_arr.text))
                        if arr_vals:
                            self.blocks[name]['Parameters'][item_name] = arr_vals

    def _try_convert(self, val_str):
        if not val_str:
            return ""
        try:
            # Check integers to avoid .0 representation
            if '.' not in val_str:
                return int(val_str)
            return float(val_str)
        except ValueError:
            if val_str.lower() == 'true': return True
            if val_str.lower() == 'false': return False
            return val_str

    def _build_tsa_dict(self):
        tsa_system = {
            "SystemName": "K-Spice Extracted Plant",
            "Models": []
        }
        
        for name, data in self.blocks.items():
            # Skip pure graphical / simulation engine blocks
            if data['Type'] in ['ThermoControl', 'DrawText', 'DrawLine', 'DrawRect', 'SimulationSettings', 'CaseSettings']:
                continue
                
            model_def = {
                "Id": name,
                "Name": name,
                "KSpiceType": data['Type'],
                "Inputs": data['Inputs'],
                "Parameters": data['Parameters']
            }
            
            # Phase 1: Direct Mapping configuration (Replaces the fragile patch_xyz scripts!)
            if data['Type'] == 'GenericASC':
                model_def["$type"] = "KSpiceRuntime.CustomModels.AntiSurgePhysicalModel, KSpiceRuntime"
            elif data['Type'] == 'ControlValve' or data['Type'] == 'ChokeValve':
                model_def["$type"] = "KSpiceRuntime.CustomModels.ValvePhysicalModel, KSpiceRuntime"
            elif data['Type'] == 'PIDController' or data['Type'] == 'PID':
                model_def["$type"] = "TimeSeriesAnalysis.Dynamic.PIDModel, TimeSeriesAnalysis"
            else:
                model_def["$type"] = "TimeSeriesAnalysis.Dynamic.UnitModel, TimeSeriesAnalysis"
                
            tsa_system['Models'].append(model_def)
            
        return tsa_system

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract K-Spice Models to JSON")
    parser.add_argument("--mdl", help="Path to .mdl or .mdl.bak")
    parser.add_argument("--prm", help="Path to .prm or .prm.bak. If omitted, it is inferred from --mdl.")
    parser.add_argument("--out", help="Output JSON map. Defaults to data/extracted/KSpiceSystemMap.json")
    parser.add_argument("--model-name", help="Optional model basename to search for when --mdl is not supplied, e.g. Com_2xKP_2xHP_Rev4")
    args = parser.parse_args()

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

    def resolve_existing_path(path_value):
        if not path_value:
            return None
        candidates = [path_value]
        if not os.path.isabs(path_value):
            candidates.append(os.path.abspath(os.path.join(base_dir, path_value)))
        for candidate in candidates:
            if os.path.exists(candidate):
                return os.path.abspath(candidate)
        return None

    def find_model_file(model_name):
        if not model_name:
            return None
        search_roots = [base_dir, os.path.join(base_dir, 'kspicefiles'), os.path.dirname(base_dir)]
        patterns = [f"{model_name}.mdl", f"{model_name}.mdl.bak", f"{model_name}.xml", f"{model_name}*.mdl", f"{model_name}*.mdl.bak"]
        for root in search_roots:
            for pattern in patterns:
                matches = glob.glob(os.path.join(root, '**', pattern), recursive=True)
                if matches:
                    return os.path.abspath(matches[0])
        return None

    mdl_path = resolve_existing_path(args.mdl) or find_model_file(args.model_name)
    if not mdl_path:
        print("[ERROR] No input model file was found.")
        print("Provide --mdl with a real .mdl/.mdl.bak path, or --model-name to search for it.")
        sys.exit(1)

    prm_path = resolve_existing_path(args.prm)
    if not prm_path:
        if mdl_path.endswith('.mdl.bak'):
            inferred_prm = mdl_path.replace('.mdl.bak', '.prm.bak')
        else:
            inferred_prm = os.path.splitext(mdl_path)[0] + '.prm'
        prm_path = inferred_prm if os.path.exists(inferred_prm) else None

    out_path = resolve_existing_path(args.out) or args.out
    if not out_path:
        out_path = os.path.join(base_dir, 'data', 'extracted', 'KSpiceSystemMap.json')
    if not os.path.isabs(out_path):
        out_path = os.path.abspath(os.path.join(base_dir, out_path))

    print(f"Using model: {mdl_path}")
    if prm_path:
        print(f"Using parameters: {prm_path}")
    else:
        print("[WARN] No PRM file found; continuing without parameter import.")

    extractor = KSpiceParser(mdl_path, prm_path)
    result = extractor.parse()
    
    print("Writing extracted topology to:", out_path)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    
    if "Models" in result:
        print(f"Extracted {len(result['Models'])} viable components successfully.")
