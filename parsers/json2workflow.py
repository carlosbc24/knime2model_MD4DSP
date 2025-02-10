import os
import json
from xml.dom import minidom
import xml.etree.ElementTree as ET


def build_input_port(parent, base_name, node_name, index, fields):
    """
    Crea el elemento inputPort y sus hijos (datafield y dataDictionaryDefinition).
    """
    input_port = ET.SubElement(parent, "inputPort", {
        "fileName": f"{base_name.lower().replace(' ', '_')}_input_dataDictionary.csv",
        "name": f"{base_name}_input_dataDictionary",
        "out": f"//@dataprocessing.{index}/@outputPort.0"
    })
    for i, field in enumerate(fields):
        df = ET.SubElement(input_port, "datafield", {
            "xsi:type": "Workflow:Categorical",
            "name": f"{base_name}({field})_input_dataField" if len(fields) > 1 else f"{node_name}_input_dataField",
            "displayName": field,
            "out": f"//@dataprocessing.{index}/@outputPort.0/@datafield.{i}"
        })
        ET.SubElement(df, "categoricalDef", {
            "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@inputPort.0/@datafielddefinition.{i}"
        })
    ET.SubElement(input_port, "dataDictionaryDefinition", {
        "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@inputPort.0"
    })
    return input_port


def build_output_port(parent, base_name, node_name, index, fields):
    """
    Crea el elemento outputPort y sus hijos (datafield y dataDictionaryDefinition).
    """
    output_port = ET.SubElement(parent, "outputPort", {
        "fileName": f"{base_name.lower().replace(' ', '_')}_output_dataDictionary.csv",
        "name": f"{base_name}_output_dataDictionary",
        "in": f"//@dataprocessing.{index}/@inputPort.0"
    })
    for i, field in enumerate(fields):
        df_out = ET.SubElement(output_port, "datafield", {
            "xsi:type": "Workflow:Categorical",
            "name": f"{base_name}({field})_output_dataField" if len(fields) > 1 else f"{node_name}_output_dataField",
            "displayName": field,
            "in": f"//@dataprocessing.{index}/@inputPort.0/@datafield.{i}"
        })
        ET.SubElement(df_out, "categoricalDef", {
            "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@outputPort.0/@datafielddefinition.{i}"
        })
    ET.SubElement(output_port, "dataDictionaryDefinition", {
        "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@outputPort.0"
    })
    return output_port


def build_parameters(parent, node_name, parameters, index):
    """
    Crea los elementos <parameter> (y su hijo <derivedValueDef>) a partir de los parámetros del nodo.
    """
    for param_key, param_value in parameters.items():
        # Si el valor es complejo (lista o dict) se usa json.dumps, de lo contrario str()
        param_value_str = json.dumps(param_value) if isinstance(param_value, (list, dict)) else str(param_value)
        param_elem = ET.SubElement(parent, "parameter", {
            "xsi:type": "Workflow:DerivedValue",
            "name": f"{node_name}_param_{param_key}",
            "value": param_value_str
        })
        ET.SubElement(param_elem, "derivedValueDef", {
            "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}/@parameterdefinition.0"
        })


def build_node(node, index):
    """
    Procesa un nodo "normal" (no Reader/Writer) del JSON y devuelve:
      - node_id: identificador del nodo (o su índice)
      - dp: el elemento XML 'dataprocessing' generado
      - node_name: el nombre del nodo (para referencia en los enlaces)
    """
    node_id = node.get("id", index)
    node_name = node.get("node_name", f"Node_{index}")

    # Crear elemento dataprocessing
    dp = ET.Element("dataprocessing", {
        "xsi:type": "Workflow:DataProcessing",
        "name": node_name
    })
    # dp.set("incoming", "")
    # dp.set("outgoing", "")

    # Determinar base_name y campos (fields) a partir del nombre del nodo
    if "(" in node_name and ")" in node_name:
        base_name = node_name.split("(")[0].strip()
        inner = node_name[node_name.find("(") + 1: node_name.find(")")]
        fields = [f.strip() for f in inner.split(",") if f.strip()]
    else:
        base_name = node_name
        fields = [node_name]

    # # Crear inputPort y outputPort
    # build_input_port(dp, base_name, node_name, index, fields)
    # build_output_port(dp, base_name, node_name, index, fields)
    #
    # # Establecer atributos 'in' y 'out' a partir de las referencias de cada datafield
    # input_refs = [f"//@dataprocessing.{index}/@inputPort.0/@datafield.{i}" for i in range(len(fields))]
    # output_refs = [f"//@dataprocessing.{index}/@outputPort.0/@datafield.{i}" for i in range(len(fields))]
    # dp.set("in", " ".join(input_refs))
    # dp.set("out", " ".join(output_refs))

    # # Agregar definición de transformación
    # ET.SubElement(dp, "dataProcessingDefinition", {
    #     "xsi:type": "Library:Transformation",
    #     "href": f"library_validation.xmi#//@dataprocessingdefinition.{index}"
    # })

    # Agregar parámetros (si existen)
    # build_parameters(dp, node_name, node.get("parameters", {}), index)

    return node_id, dp, node_name


def build_link(link_index, conn, node_mapping):
    """
    Procesa una conexión (enlace) entre nodos "normales" y actualiza los atributos 'incoming' y 'outgoing'
    de los elementos de los nodos fuente y destino. Devuelve el elemento <link> generado.
    """
    source_id = conn.get("sourceID")
    dest_id = conn.get("destID")
    print("Link_index:", link_index, "Source:", source_id, "Dest:", dest_id)
    # Sólo se crean links si ambos nodos son "normales"
    if source_id in node_mapping and dest_id in node_mapping:
        source_info = node_mapping[source_id]
        dest_info = node_mapping[dest_id]
        link_element = ET.Element("link", {
            "source": f"//@dataprocessing.{source_info['index']}",
            "target": f"//@dataprocessing.{dest_info['index']}",
            "name": f"{source_info['name']}-{dest_info['name']}"
        })
        link_ref = f"//@link.{link_index}"
        # Actualizar atributo outgoing del nodo fuente
        current_out = source_info["element"].get("outgoing")
        if current_out:
            source_info["element"].set("outgoing", current_out + " " + link_ref)
        else:
            source_info["element"].set("outgoing", link_ref)
        # Actualizar atributo incoming del nodo destino
        current_in = dest_info["element"].get("incoming")
        if current_in:
            dest_info["element"].set("incoming", current_in + " " + link_ref)
        else:
            dest_info["element"].set("incoming", link_ref)
        return link_element
    return None


def json_to_xmi_workflow(json_input_filepath, xmi_output_path, xmi_output_filename,
                         workflow_name="Model data set with metanode (KNIME)"):
    """
    Convierte un JSON con la estructura de un workflow KNIME a un archivo XMI bien formateado.
    Se procesan los nodos de forma modular; los nodos cuyo nombre termina en "Reader" o "Writer"
    NO se transforman en un elemento <dataprocessing> sino que su file_path se inyecta en el
    <inputPort> o <outputPort> del nodo conectado (posterior o anterior, respectivamente).
    """
    # Cargar datos JSON
    with open(json_input_filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Registrar namespaces
    ns = {
        "xmi": "http://www.omg.org/XMI",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "Library": "http://www.example.org/Library",
        "Workflow": "https://www.example.org/workflow"
    }
    for prefix, uri in ns.items():
        ET.register_namespace(prefix, uri)

    # Crear elemento raíz (Workflow)
    root = ET.Element("{https://www.example.org/workflow}Workflow", {
                      "{http://www.omg.org/XMI}version": "2.0",
                      "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation":
                      "http://www.example.org/Library ../metamodel/Library.ecore https://www.example.org/workflow ../metamodel/Workflow.ecore",
                      "name": workflow_name,
                      "xmlns:Library": "http://www.example.org/Library",
    })

    # Diccionarios para nodos "normales" y para nodos especiales (Reader/Writer)
    node_mapping = {}      # nodos que se transforman en <dataprocessing>
    reader_mapping = {}    # id -> file_path (para nodos Reader)
    writer_mapping = {}    # id -> file_path (para nodos Writer)

    nodes = data.get("nodes", [])
    for index, node in enumerate(nodes):
        node_id = node.get("id", index)
        node_name = node.get("node_name", f"Node_{index}")
        # Detectar si el nodo es un Reader o Writer (se usa endswith; se puede ajustar la lógica)
        if node_name.strip().endswith("Reader"):
            file_path = node.get("parameters", {}).get("file_path", "")
            reader_mapping[node_id] = file_path
            # No se crea elemento <dataprocessing>
            continue
        elif node_name.strip().endswith("Writer"):
            file_path = node.get("parameters", {}).get("file_path", "")
            writer_mapping[node_id] = file_path
            continue
        else:
            # Nodo "normal": se transforma en <dataprocessing>
            n_id, dp_element, n_name = build_node(node, index)
            root.append(dp_element)
            node_mapping[node_id] = {"element": dp_element, "index": index, "name": n_name}

    # Procesar conexiones
    links = data.get("connections", [])
    for link_index, conn in enumerate(links):
        source_id = conn.get("sourceID")
        dest_id = conn.get("destID")
        # Caso 1: conexión desde un nodo Reader a un nodo "normal"
        if source_id in reader_mapping and dest_id in node_mapping:
            target_node = node_mapping[dest_id]["element"]
            input_port = target_node.find("inputPort")
            if input_port is not None:
                # Se sobrescribe el atributo fileName con el file_path del Reader
                input_port.set("fileName", reader_mapping[source_id])
            continue  # No se crea elemento <link>
        # Caso 2: conexión desde un nodo "normal" a un nodo Writer
        if dest_id in writer_mapping and source_id in node_mapping:
            source_node = node_mapping[source_id]["element"]
            output_port = source_node.find("outputPort")
            if output_port is not None:
                output_port.set("fileName", writer_mapping[dest_id])
            continue  # No se crea elemento <link>
        # Caso 3: conexión entre dos nodos "normales"
        if source_id in node_mapping and dest_id in node_mapping:
            link_element = build_link(link_index, conn, node_mapping)
            if link_element is not None:
                root.append(link_element)

    # Convert XML to string and format it
    raw_xml = ET.tostring(root, encoding='utf-8')
    parsed = minidom.parseString(raw_xml)
    formatted_xml = "\n".join(line for line in parsed.toprettyxml(indent="    ").split("\n") if line.strip())

    # Save formatted XML to file
    output_xmi_filepath = os.path.join(xmi_output_path, xmi_output_filename)
    os.makedirs(os.path.dirname(output_xmi_filepath), exist_ok=True)
    with open(output_xmi_filepath, "w", encoding="utf-8") as file:
        file.write(formatted_xml)

    print(f"Workflow XMI saved to: {xmi_output_path}/{xmi_output_filename}")
