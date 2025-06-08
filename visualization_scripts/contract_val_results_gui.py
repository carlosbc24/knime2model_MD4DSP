import gradio as gr
import json
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from collections import defaultdict, Counter
import random

# === Configuración ===

DATA_PATHS = {
    "KNIME": "visualization_scripts/34_workflow_generated_code_validation_KNIME_data.json",
    "Python": "visualization_scripts/34_workflow_generated_code_validation_Python_data.json"
}
EXPORT_DIR = "visualization_scripts/barcharts_images"
os.makedirs(EXPORT_DIR, exist_ok=True)

# === Carga de datos ===

def flatten_contracts(contract_list: list):
    """
    Devuelve una lista de tuplas (tipo, resultado) para todos los contratos de un nodo.
    """
    result = []
    for d in contract_list:
        if isinstance(d, dict):
            for k, v in d.items():
                result.append((k, v))
    return result

def load_data(source: str) -> list:
    with open(DATA_PATHS[source], 'r') as f:
        return json.load(f)["workflows"]

# === Asignación de colores ===

def assign_colors(result_types):
    fixed_colors = {
        "VALIDATED": "green",
        "NOT VALIDATED": "red"
    }
    others = [res for res in result_types if res not in fixed_colors]
    random.seed(42)
    palette = [
        "#1f77b4", "#ff7f0e", "#d62728",
        "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
        "#bcbd22", "#17becf"
    ]
    random.shuffle(palette)
    color_map = fixed_colors.copy()
    for res, col in zip(others, palette):
        color_map[res] = col
    return color_map

# === Generación de gráficos y estadísticas ===

def generate_bar_chart_and_stats(source: str, x_axis_type: str):
    workflows = load_data(source)

    # Identificar todos los tipos de resultado
    all_results = set()
    for wf in workflows:
        for node in wf["nodes"]:
            for contracts in node.values():
                for _, result in flatten_contracts(contracts):
                    all_results.add(result)
    all_results = sorted(all_results)
    color_map = assign_colors(all_results)

    def count_contracts(nodes):
        counts = {res: 0 for res in all_results}
        for node in nodes:
            for contracts in node.values():
                for _, result in flatten_contracts(contracts):
                    if result in counts:
                        counts[result] += 1
        return counts

    result_counts = {res: [] for res in all_results}
    x_labels = []

    if x_axis_type == "subworkflows":
        fig, ax = plt.subplots(figsize=(8, 6))
        for wf in workflows:
            x_labels.append(wf["name"])
            counts = count_contracts(wf["nodes"])
            for res in all_results:
                result_counts[res].append(counts[res])

    elif x_axis_type == "subworkflows by node types":
        fig, ax = plt.subplots(figsize=(8, 6))
        node_type_set = set()
        for wf in workflows:
            for node in wf["nodes"]:
                node_type_set.update(node.keys())
        node_types = sorted(node_type_set)
        color_map = assign_colors(node_types)
        node_type_counts = {nt: [] for nt in node_types}
        x_labels = []
        for wf in workflows:
            x_labels.append(wf["name"])
            counts = Counter()
            for node in wf["nodes"]:
                for node_type, contracts in node.items():
                    counts[node_type] += 1
            for nt in node_types:
                node_type_counts[nt].append(counts[nt])
        x = range(len(x_labels))
        left = [0] * len(x_labels)
        for nt in node_types:
            ax.barh(x, node_type_counts[nt], left=left, color=color_map[nt], label=nt)
            left = [l + c for l, c in zip(left, node_type_counts[nt])]
        ax.set_yticks(x)
        ax.set_yticklabels(x_labels)
        ax.set_xlabel("Number of nodes")
        ax.set_title("Node types in subworkflows")
        ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), fontsize="small")
        ax.xaxis.get_major_locator().set_params(integer=True)
        max_width = max(sum(values) for values in zip(*node_type_counts.values()))
        ax.set_xlim(right=max_width * 1.1)
        plt.subplots_adjust(right=0.75)
        plt.tight_layout()
        filename = f"chart_{source}_subworkflows_by_node_types.png"
        file_path = os.path.join(EXPORT_DIR, filename)
        plt.savefig(file_path)
        plt.close(fig)
        stats_html = "<h4>node types Statistics</h4>"
        total_nodes = sum(sum(v) for v in node_type_counts.values())
        for nt in node_types:
            count = sum(node_type_counts[nt])
            percent = 100 * count / total_nodes if total_nodes else 0
            stats_html += f"<p style='color:{color_map[nt]}'><b>{nt}</b>: {count} ({percent:.1f}%)</p>"
        return file_path, stats_html

    elif x_axis_type == "contract type":
        fig, ax = plt.subplots(figsize=(8, 6))
        contract_types = ["PRECONDITION", "POSTCONDITION", "INVARIANT"]
        x_labels = contract_types
        for ctype in contract_types:
            c_counts = {res: 0 for res in all_results}
            for wf in workflows:
                for node in wf["nodes"]:
                    for contracts in node.values():
                        for tipo, result in flatten_contracts(contracts):
                            if tipo == ctype and result in all_results:
                                c_counts[result] += 1
            for res in all_results:
                result_counts[res].append(c_counts[res])

    elif x_axis_type == "node type":
        fig, ax = plt.subplots(figsize=(8, 6))
        node_type_counts = defaultdict(lambda: {res: 0 for res in all_results})
        for wf in workflows:
            for node in wf["nodes"]:
                for node_type, contracts in node.items():
                    for _, result in flatten_contracts(contracts):
                        if result in all_results:
                            node_type_counts[node_type][result] += 1
        x_labels = list(node_type_counts.keys())
        for res in all_results:
            result_counts[res] = [node_type_counts[nt][res] for nt in x_labels]

    if x_axis_type == "node type" or x_axis_type == "contract type":
        x = range(len(x_labels))
        bottom = [0] * len(x_labels)
        for res in all_results:
            ax.bar(x, result_counts[res], bottom=bottom, color=color_map[res], label=res)
            bottom = [b + rc for b, rc in zip(bottom, result_counts[res])]
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels, rotation=90)
        ax.set_ylim(top=max(sum(values) for values in zip(*result_counts.values())) * 1.1)
        ax.set_ylabel("Number of contracts")
        ax.set_title(f"Contract validation results by {x_axis_type}")
        ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), fontsize="small")
        plt.subplots_adjust(right=0.75)
        plt.tight_layout()
        filename = f"chart_{source}_{x_axis_type.replace(' ', '_')}.png"
        file_path = os.path.join(EXPORT_DIR, filename)
        plt.savefig(file_path)
        plt.close(fig)
        # Estadísticas
        total_counter = Counter()
        contract_counter = defaultdict(Counter)
        for wf in workflows:
            for node in wf["nodes"]:
                for contracts in node.values():
                    for tipo, result in flatten_contracts(contracts):
                        total_counter[result] += 1
                        contract_counter[tipo][result] += 1
        stats_html = "<h4>Validation Statistics</h4>"
        stats_html += f"<p><b>Total contracts:</b> {sum(total_counter.values())}</p>"
        for res in all_results:
            percent = 100 * total_counter[res] / sum(total_counter.values())
            stats_html += f"<p style='color:{color_map[res]}'><b>{res}</b>: {total_counter[res]} ({percent:.1f}%)</p>"
        stats_html += "<hr><h5>by contract type:</h5>"
        for ctype in ["PRECONDITION", "POSTCONDITION", "INVARIANT"]:
            stats_html += f"<p><b>{ctype}</b></p><ul>"
            total = sum(contract_counter[ctype].values())
            for res in all_results:
                count = contract_counter[ctype][res]
                pct = 100 * count / total if total > 0 else 0
                stats_html += f"<li style='color:{color_map[res]}'>{res}: {count} ({pct:.1f}%)</li>"
            stats_html += "</ul>"
        return file_path, stats_html

    elif x_axis_type == "subworkflows":
        x = range(len(x_labels))
        left = [0] * len(x_labels)
        for res in all_results:
            ax.barh(x, result_counts[res], left=left, color=color_map[res], label=res)
            left = [l + rc for l, rc in zip(left, result_counts[res])]
        ax.set_yticks(x)
        ax.set_yticklabels(x_labels)
        max_total = max(sum(values) for values in zip(*result_counts.values()))
        ax.set_xlim(right=(int(max_total) + 2))
        ax.set_xlabel("Number of contracts")
        ax.set_title(f"Contract validation results by {x_axis_type}")
        ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), fontsize="small")
        plt.subplots_adjust(right=0.75)
        plt.tight_layout()
        filename = f"chart_{source}_{x_axis_type.replace(' ', '_')}.png"
        file_path = os.path.join(EXPORT_DIR, filename)
        plt.savefig(file_path)
        plt.close(fig)
        # Estadísticas
        total_counter = Counter()
        contract_counter = defaultdict(Counter)
        for wf in workflows:
            for node in wf["nodes"]:
                for contracts in node.values():
                    for tipo, result in flatten_contracts(contracts):
                        total_counter[result] += 1
                        contract_counter[tipo][result] += 1
        stats_html = "<h4>Validation Statistics</h4>"
        stats_html += f"<p><b>Total contracts:</b> {sum(total_counter.values())}</p>"
        for res in all_results:
            percent = 100 * total_counter[res] / sum(total_counter.values())
            stats_html += f"<p style='color:{color_map[res]}'><b>{res}</b>: {total_counter[res]} ({percent:.1f}%)</p>"
        stats_html += "<hr><h5>by contract type:</h5>"
        for ctype in ["PRECONDITION", "POSTCONDITION", "INVARIANT"]:
            stats_html += f"<p><b>{ctype}</b></p><ul>"
            total = sum(contract_counter[ctype].values())
            for res in all_results:
                count = contract_counter[ctype][res]
                pct = 100 * count / total if total > 0 else 0
                stats_html += f"<li style='color:{color_map[res]}'>{res}: {count} ({pct:.1f}%)</li>"
            stats_html += "</ul>"
        return file_path, stats_html

# === Exportación de imagen ===

def export_image():
    latest_image = next(
        (f for f in sorted(os.listdir(EXPORT_DIR), reverse=True) if f.startswith("chart_") and f.endswith(".png")),
        None
    )
    if latest_image:
        export_path = os.path.join(EXPORT_DIR, "exported_chart.png")
        latest_path = os.path.join(EXPORT_DIR, latest_image)
        os.replace(latest_path, export_path)
        return f"Chart exported as: {export_path}"
    return "No chart available for export."

# === GUI ===

with gr.Blocks(title="Contract Validation Results for 34 subworkflows") as demo:
    gr.Markdown("## Contract Validation Results for 34 subworkflows")
    with gr.Row():
        with gr.Column(scale=2):
            source = gr.Radio(choices=["KNIME", "Python"], value="KNIME", label="Select the platform")
            x_axis = gr.Radio(
                choices=["node type", "contract type", "subworkflows", "subworkflows by node types"],
                value="subworkflows",
                label="Select X-axis variable"
            )
            chart = gr.Image(type="filepath", label="Generated Chart")
            export_btn = gr.Button("Export Image")
            export_msg = gr.Textbox(label="Export Message")
        with gr.Column(scale=1):
            stats_panel = gr.HTML(label="Validation Statistics")
    source.change(fn=generate_bar_chart_and_stats, inputs=[source, x_axis], outputs=[chart, stats_panel])
    x_axis.change(fn=generate_bar_chart_and_stats, inputs=[source, x_axis], outputs=[chart, stats_panel])
    export_btn.click(fn=export_image, inputs=[], outputs=export_msg)
    chart.value, stats_panel.value = generate_bar_chart_and_stats("KNIME", "subworkflows")
if __name__ == "__main__":
    demo.launch()