"""
Process map visualization module for Streamlit.

Uses pm4py to discover and render DFG, BPMN, Petri net, and Process Tree
visualizations. Functions return image bytes (PNG) via BytesIO so Streamlit
can display them with st.image(), DOT source strings for st.graphviz_chart(),
or interactive HTML with JavaScript pan/zoom/fit for st.components.v1.html().
"""

import io
import base64
from typing import Dict, Optional, Tuple

import pandas as pd


def _convert_df_to_event_log(log_df: pd.DataFrame):
    """
    Convert a pandas DataFrame to a pm4py event log object.

    Expects standard process mining columns: case:concept:name,
    concept:name, time:timestamp.

    Args:
        log_df: DataFrame with event log data.

    Returns:
        A pm4py EventLog object.
    """
    from pm4py.objects.log.util import dataframe_utils
    from pm4py.objects.conversion.log import converter as log_converter

    df = log_df.copy()
    df = dataframe_utils.convert_timestamp_columns_in_df(df)

    parameters = {
        log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case:concept:name'
    }
    event_log = log_converter.apply(
        df, parameters=parameters,
        variant=log_converter.Variants.TO_EVENT_LOG,
    )
    return event_log


def _gviz_to_png_bytes(gviz) -> bytes:
    """
    Convert a pm4py graphviz visualization object to PNG bytes.

    Handles the different return types that pm4py visualizers may produce:
    a graphviz.Source object (with .pipe()), a raw string, or an object
    with a .render() method.

    Args:
        gviz: Visualization object returned by a pm4py visualizer.

    Returns:
        PNG image data as bytes.
    """
    if hasattr(gviz, 'pipe'):
        # graphviz.Source or graphviz.Digraph -- render directly to PNG
        return gviz.pipe(format='png')

    if isinstance(gviz, bytes):
        return gviz

    if isinstance(gviz, str):
        # It might be SVG content or DOT source; try to compile it via graphviz
        import graphviz
        src = graphviz.Source(gviz)
        return src.pipe(format='png')

    # Last resort: if the object has a render method
    if hasattr(gviz, 'render'):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            rendered_path = gviz.render(filename=tmp_path, format='png', cleanup=True)
            with open(rendered_path, 'rb') as f:
                return f.read()
        finally:
            for p in [tmp_path, tmp_path + '.png']:
                if os.path.exists(p):
                    os.remove(p)

    raise TypeError(
        f"Cannot convert visualization object of type {type(gviz)} to PNG bytes."
    )


def _gviz_to_dot_source(gviz) -> str:
    """
    Extract DOT source string from a pm4py graphviz visualization object.

    Args:
        gviz: Visualization object returned by a pm4py visualizer.

    Returns:
        DOT language source string.
    """
    if hasattr(gviz, 'source'):
        return gviz.source

    if isinstance(gviz, str):
        return gviz

    raise TypeError(
        f"Cannot extract DOT source from object of type {type(gviz)}."
    )


def _gviz_to_svg_string(gviz) -> str:
    """
    Convert a pm4py graphviz visualization object to an SVG string.

    Args:
        gviz: Visualization object returned by a pm4py visualizer.

    Returns:
        SVG markup as a string.
    """
    if hasattr(gviz, 'pipe'):
        svg_bytes = gviz.pipe(format='svg')
        return svg_bytes.decode('utf-8')

    if isinstance(gviz, str):
        import graphviz
        src = graphviz.Source(gviz)
        return src.pipe(format='svg').decode('utf-8')

    raise TypeError(
        f"Cannot convert object of type {type(gviz)} to SVG."
    )


def wrap_svg_interactive(svg_string: str, height: int = 650) -> str:
    """
    Wrap an SVG string in an HTML page with JavaScript pan/zoom/fit controls.

    Features:
    - Zoom +/- buttons
    - Fit-to-view button
    - Mouse wheel zoom (centered on cursor)
    - Click-and-drag panning
    - Auto-fit on initial load
    - No external dependencies (pure JavaScript)

    Args:
        svg_string: Raw SVG markup.
        height: Pixel height for the component.

    Returns:
        Complete HTML string for use with streamlit.components.v1.html().
    """
    # Encode SVG as base64 to avoid escaping issues in the HTML template
    svg_b64 = base64.b64encode(svg_string.encode('utf-8')).decode('ascii')

    html = f"""<!DOCTYPE html>
<html>
<head>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ overflow: hidden; background: #ffffff; }}
  #container {{
    width: 100%; height: {height}px; position: relative;
    overflow: hidden; cursor: grab; border: 1px solid #e0e0e0;
    border-radius: 4px; background: #fafafa;
  }}
  #container.dragging {{ cursor: grabbing; }}
  #svg-wrapper {{
    position: absolute; top: 0; left: 0;
    transform-origin: 0 0;
    visibility: hidden;
  }}
  #controls {{
    position: absolute; top: 8px; right: 8px; z-index: 10;
    display: flex; flex-direction: column; gap: 4px;
  }}
  #controls button {{
    width: 32px; height: 32px; border: 1px solid #ccc;
    border-radius: 4px; background: #fff; cursor: pointer;
    font-size: 16px; line-height: 1; display: flex;
    align-items: center; justify-content: center;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
  }}
  #controls button:hover {{ background: #f0f0f0; }}
  #zoom-info {{
    position: absolute; bottom: 8px; right: 8px; z-index: 10;
    font-size: 11px; color: #888; font-family: monospace;
    background: rgba(255,255,255,0.8); padding: 2px 6px;
    border-radius: 3px;
  }}
</style>
</head>
<body>
<div id="container">
  <div id="svg-wrapper"></div>
  <div id="controls">
    <button id="btn-zoomin" title="Zoom in">+</button>
    <button id="btn-zoomout" title="Zoom out">&minus;</button>
    <button id="btn-fit" title="Fit to view">&#x2922;</button>
  </div>
  <div id="zoom-info"></div>
</div>
<script>
(function() {{
  // Decode and inject SVG (UTF-8 aware base64 decode)
  var svgB64 = "{svg_b64}";
  var bytes = Uint8Array.from(atob(svgB64), function(c) {{ return c.charCodeAt(0); }});
  var svgText = new TextDecoder('utf-8').decode(bytes);
  var wrapper = document.getElementById('svg-wrapper');
  wrapper.innerHTML = svgText;

  var container = document.getElementById('container');
  var zoomInfo = document.getElementById('zoom-info');
  var svgEl = wrapper.querySelector('svg');

  // Parse natural SVG dimensions from width/height attributes (e.g. "1234pt")
  // Graphviz uses "pt" units: 1pt = 1.33px. We keep the SVG at natural size
  // and use CSS transform on the wrapper to scale/pan.
  var naturalW = 800, naturalH = 600;
  if (svgEl) {{
    svgEl.style.display = 'block';
    var wAttr = svgEl.getAttribute('width');
    var hAttr = svgEl.getAttribute('height');
    // parseFloat handles "1234pt", "1234px", "1234" -> 1234
    if (wAttr) {{
      var pw = parseFloat(wAttr);
      // Convert pt to px if unit is "pt" (1pt = 1.333px)
      if (wAttr.indexOf('pt') !== -1) pw *= 1.333;
      if (pw > 0) naturalW = pw;
    }}
    if (hAttr) {{
      var ph = parseFloat(hAttr);
      if (hAttr.indexOf('pt') !== -1) ph *= 1.333;
      if (ph > 0) naturalH = ph;
    }}
    // Set explicit pixel dimensions so the SVG renders at a known size
    svgEl.setAttribute('width', naturalW + 'px');
    svgEl.setAttribute('height', naturalH + 'px');
  }}

  var scale = 1, tx = 0, ty = 0;
  var dragging = false, startX = 0, startY = 0, startTx = 0, startTy = 0;
  var MIN_SCALE = 0.05, MAX_SCALE = 10;

  function applyTransform() {{
    wrapper.style.transform = 'translate(' + tx + 'px,' + ty + 'px) scale(' + scale + ')';
    zoomInfo.textContent = Math.round(scale * 100) + '%';
  }}

  function fitToView() {{
    var cw = container.clientWidth - 16;
    var ch = container.clientHeight - 16;
    if (cw <= 0 || ch <= 0) return false;
    scale = Math.min(cw / naturalW, ch / naturalH, 2);
    tx = (cw - naturalW * scale) / 2 + 8;
    ty = (ch - naturalH * scale) / 2 + 8;
    applyTransform();
    wrapper.style.visibility = 'visible';
    return true;
  }}

  // Robust auto-fit: retry until the container has real dimensions
  (function autoFit(attempt) {{
    if (fitToView()) return;
    if (attempt < 20) requestAnimationFrame(function() {{ autoFit(attempt + 1); }});
  }})(0);

  // Button handlers
  document.getElementById('btn-zoomin').addEventListener('click', function() {{
    var cx = container.clientWidth / 2;
    var cy = container.clientHeight / 2;
    var newScale = Math.min(scale * 1.3, MAX_SCALE);
    var ratio = newScale / scale;
    tx = cx - ratio * (cx - tx);
    ty = cy - ratio * (cy - ty);
    scale = newScale;
    applyTransform();
  }});

  document.getElementById('btn-zoomout').addEventListener('click', function() {{
    var cx = container.clientWidth / 2;
    var cy = container.clientHeight / 2;
    var newScale = Math.max(scale / 1.3, MIN_SCALE);
    var ratio = newScale / scale;
    tx = cx - ratio * (cx - tx);
    ty = cy - ratio * (cy - ty);
    scale = newScale;
    applyTransform();
  }});

  document.getElementById('btn-fit').addEventListener('click', fitToView);

  // Mouse wheel zoom (centered on cursor position)
  container.addEventListener('wheel', function(e) {{
    e.preventDefault();
    var rect = container.getBoundingClientRect();
    var mx = e.clientX - rect.left;
    var my = e.clientY - rect.top;
    var factor = e.deltaY < 0 ? 1.15 : 1 / 1.15;
    var newScale = Math.max(MIN_SCALE, Math.min(MAX_SCALE, scale * factor));
    var ratio = newScale / scale;
    tx = mx - ratio * (mx - tx);
    ty = my - ratio * (my - ty);
    scale = newScale;
    applyTransform();
  }}, {{ passive: false }});

  // Drag to pan
  container.addEventListener('mousedown', function(e) {{
    if (e.target.tagName === 'BUTTON') return;
    dragging = true;
    startX = e.clientX; startY = e.clientY;
    startTx = tx; startTy = ty;
    container.classList.add('dragging');
    e.preventDefault();
  }});

  window.addEventListener('mousemove', function(e) {{
    if (!dragging) return;
    tx = startTx + (e.clientX - startX);
    ty = startTy + (e.clientY - startY);
    applyTransform();
  }});

  window.addEventListener('mouseup', function() {{
    dragging = false;
    container.classList.remove('dragging');
  }});
}})();
</script>
</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# Interactive renderers (return HTML string for st.components.v1.html)
# ---------------------------------------------------------------------------

def render_dfg_interactive(
    dfg: Dict[Tuple[str, str], int],
    start_activities: Dict[str, int],
    end_activities: Dict[str, int],
    performance: Optional[Dict[Tuple[str, str], float]] = None,
    height: int = 650,
) -> str:
    """
    Render a DFG as interactive HTML with pan/zoom/fit controls.

    Returns:
        HTML string for use with streamlit.components.v1.html().
    """
    from pm4py.visualization.dfg import visualizer as dfg_visualizer

    if performance:
        variant = dfg_visualizer.Variants.PERFORMANCE
        parameters = {
            variant.value.Parameters.FORMAT: 'svg',
            variant.value.Parameters.START_ACTIVITIES: start_activities,
            variant.value.Parameters.END_ACTIVITIES: end_activities,
        }
        gviz = dfg_visualizer.apply(performance, parameters=parameters, variant=variant)
    else:
        variant = dfg_visualizer.Variants.FREQUENCY
        parameters = {
            variant.value.Parameters.FORMAT: 'svg',
            variant.value.Parameters.START_ACTIVITIES: start_activities,
            variant.value.Parameters.END_ACTIVITIES: end_activities,
        }
        gviz = dfg_visualizer.apply(dfg, parameters=parameters, variant=variant)

    svg_string = _gviz_to_svg_string(gviz)
    return wrap_svg_interactive(svg_string, height)


def render_bpmn_interactive(event_log, height: int = 650) -> str:
    """
    Discover a BPMN model and render it as interactive HTML.

    Args:
        event_log: pm4py EventLog object or DataFrame.
    Returns:
        HTML string for use with streamlit.components.v1.html().
    """
    if isinstance(event_log, pd.DataFrame):
        event_log = _convert_df_to_event_log(event_log)

    import pm4py
    bpmn_model = pm4py.discover_bpmn_inductive(event_log)
    from pm4py.visualization.bpmn import visualizer as bpmn_visualizer
    gviz = bpmn_visualizer.apply(bpmn_model)
    svg_string = _gviz_to_svg_string(gviz)
    return wrap_svg_interactive(svg_string, height)


def render_petri_net_interactive(event_log, height: int = 650) -> str:
    """
    Discover a Petri net and render it as interactive HTML.

    Args:
        event_log: pm4py EventLog object or DataFrame.
    Returns:
        HTML string for use with streamlit.components.v1.html().
    """
    if isinstance(event_log, pd.DataFrame):
        event_log = _convert_df_to_event_log(event_log)

    import pm4py
    net, im, fm = pm4py.discover_petri_net_inductive(event_log)
    from pm4py.visualization.petri_net import visualizer as pn_visualizer
    gviz = pn_visualizer.apply(net, im, fm)
    svg_string = _gviz_to_svg_string(gviz)
    return wrap_svg_interactive(svg_string, height)


def render_process_tree_interactive(event_log, height: int = 650) -> str:
    """
    Discover a Process Tree and render it as interactive HTML.

    Args:
        event_log: pm4py EventLog object or DataFrame.
    Returns:
        HTML string for use with streamlit.components.v1.html().
    """
    if isinstance(event_log, pd.DataFrame):
        event_log = _convert_df_to_event_log(event_log)

    import pm4py
    process_tree = pm4py.discover_process_tree_inductive(event_log)
    from pm4py.visualization.process_tree import visualizer as pt_visualizer
    gviz = pt_visualizer.apply(process_tree)
    svg_string = _gviz_to_svg_string(gviz)
    return wrap_svg_interactive(svg_string, height)


# ---------------------------------------------------------------------------
# DFG rendering (PNG - legacy)
# ---------------------------------------------------------------------------

def render_dfg(
    dfg: Dict[Tuple[str, str], int],
    start_activities: Dict[str, int],
    end_activities: Dict[str, int],
    performance: Optional[Dict[Tuple[str, str], float]] = None,
) -> bytes:
    """
    Render a Directly-Follows Graph as a PNG image.

    Args:
        dfg: DFG dictionary mapping (source, target) pairs to frequencies.
        start_activities: Dictionary of start activities and their counts.
        end_activities: Dictionary of end activities and their counts.
        performance: Optional dictionary mapping (source, target) pairs to
            average durations (in seconds). When provided, the performance
            variant is used instead of frequency.

    Returns:
        PNG image data as bytes, suitable for st.image().
    """
    from pm4py.visualization.dfg import visualizer as dfg_visualizer

    if performance:
        variant = dfg_visualizer.Variants.PERFORMANCE
        parameters = {
            variant.value.Parameters.FORMAT: 'png',
            variant.value.Parameters.START_ACTIVITIES: start_activities,
            variant.value.Parameters.END_ACTIVITIES: end_activities,
        }
        gviz = dfg_visualizer.apply(performance, parameters=parameters, variant=variant)
    else:
        variant = dfg_visualizer.Variants.FREQUENCY
        parameters = {
            variant.value.Parameters.FORMAT: 'png',
            variant.value.Parameters.START_ACTIVITIES: start_activities,
            variant.value.Parameters.END_ACTIVITIES: end_activities,
        }
        gviz = dfg_visualizer.apply(dfg, parameters=parameters, variant=variant)

    return _gviz_to_png_bytes(gviz)


def render_dfg_dot(
    dfg: Dict[Tuple[str, str], int],
    start_activities: Dict[str, int],
    end_activities: Dict[str, int],
) -> str:
    """
    Render a Directly-Follows Graph and return the DOT source string.

    This is useful for Streamlit's st.graphviz_chart() which accepts DOT
    source directly and renders it interactively in the browser.

    Args:
        dfg: DFG dictionary mapping (source, target) pairs to frequencies.
        start_activities: Dictionary of start activities and their counts.
        end_activities: Dictionary of end activities and their counts.

    Returns:
        DOT source string for use with st.graphviz_chart().
    """
    from pm4py.visualization.dfg import visualizer as dfg_visualizer

    variant = dfg_visualizer.Variants.FREQUENCY
    parameters = {
        variant.value.Parameters.FORMAT: 'png',
        variant.value.Parameters.START_ACTIVITIES: start_activities,
        variant.value.Parameters.END_ACTIVITIES: end_activities,
    }
    gviz = dfg_visualizer.apply(dfg, parameters=parameters, variant=variant)

    return _gviz_to_dot_source(gviz)


# ---------------------------------------------------------------------------
# BPMN rendering
# ---------------------------------------------------------------------------

def render_bpmn(event_log) -> bytes:
    """
    Discover a BPMN model from an event log and render it as a PNG image.

    Args:
        event_log: pm4py EventLog object or DataFrame.

    Returns:
        PNG image data as bytes, suitable for st.image().
    """
    if isinstance(event_log, pd.DataFrame):
        event_log = _convert_df_to_event_log(event_log)

    import pm4py
    bpmn_model = pm4py.discover_bpmn_inductive(event_log)
    from pm4py.visualization.bpmn import visualizer as bpmn_visualizer
    gviz = bpmn_visualizer.apply(bpmn_model)
    return _gviz_to_png_bytes(gviz)


# ---------------------------------------------------------------------------
# Petri net rendering
# ---------------------------------------------------------------------------

def render_petri_net(event_log) -> bytes:
    """
    Discover a Petri net from an event log and render it as a PNG image.

    Args:
        event_log: pm4py EventLog object or DataFrame.

    Returns:
        PNG image data as bytes, suitable for st.image().
    """
    if isinstance(event_log, pd.DataFrame):
        event_log = _convert_df_to_event_log(event_log)

    import pm4py
    net, im, fm = pm4py.discover_petri_net_inductive(event_log)
    from pm4py.visualization.petri_net import visualizer as pn_visualizer
    gviz = pn_visualizer.apply(net, im, fm)
    return _gviz_to_png_bytes(gviz)


# ---------------------------------------------------------------------------
# Process Tree rendering
# ---------------------------------------------------------------------------

def render_process_tree(event_log) -> bytes:
    """
    Discover a Process Tree from an event log and render it as a PNG image.

    Args:
        event_log: pm4py EventLog object or DataFrame.

    Returns:
        PNG image data as bytes, suitable for st.image().
    """
    if isinstance(event_log, pd.DataFrame):
        event_log = _convert_df_to_event_log(event_log)

    import pm4py
    process_tree = pm4py.discover_process_tree_inductive(event_log)
    from pm4py.visualization.process_tree import visualizer as pt_visualizer
    gviz = pt_visualizer.apply(process_tree)
    return _gviz_to_png_bytes(gviz)


# ---------------------------------------------------------------------------
# BPMN XML export
# ---------------------------------------------------------------------------

def export_bpmn_xml(event_log) -> bytes:
    """
    Discover a BPMN model from an event log and export it as BPMN 2.0 XML.

    Args:
        event_log: pm4py EventLog object or DataFrame.

    Returns:
        BPMN 2.0 XML data as bytes, suitable for st.download_button().
    """
    if isinstance(event_log, pd.DataFrame):
        event_log = _convert_df_to_event_log(event_log)

    import pm4py
    bpmn_model = pm4py.discover_bpmn_inductive(event_log)
    from pm4py.objects.bpmn.exporter import exporter as bpmn_exporter
    return bpmn_exporter.serialize(bpmn_model)
