#include <fstream>

#include "boost/algorithm/string/replace.hpp"
#include "optimizer/optimizer.hpp"

namespace opossum {

// TODO: This does not inherit from AbstractVisualizer, so we need to fix the naming
// TODO: Move code to cpp
class AnimatedOptimizationVisualizer {
 protected:
  const char* const html_header = R"(
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <style type="text/css">
      html, body, a {
        background-color: #000;
        color: #fff;
      }
    </style>
    <script>animate = false;</script>
  </head>
  <body>
    <script src="https://d3js.org/d3.v4.min.js"></script>
    <script src="https://unpkg.com/viz.js@1.8.0/viz.js" type="javascript/worker"></script>
    <script src="https://unpkg.com/d3-graphviz@1.3.1/build/d3-graphviz.min.js"></script>
    After <span id="step">0</span> optimizer steps, applying next: <span id="last_applied_rule">(loading)</span><br>
    <a href="#" onclick="render();">Next</a>
    <div id="graph" style="text-align: center; zoom: 50%;"></div>

    <script>
      var index = 0;
      var graphviz = d3.select("#graph").graphviz()
          .transition(function () {
              return d3.transition("main")
                  .delay(500)
                  .duration(1500);
          })
          .on("initEnd", render);

      function render() {
          var dot = steps[index].visualization;
          graphviz
              .renderDot(dot)
              .on("end", function () {
                  document.getElementById('step').innerHTML = index;
                  document.getElementById('last_applied_rule').innerHTML = steps[(index + 1) % steps.length].last_applied_rule;
                  index = (index + 1) % steps.length;
              });
      }

      var steps = [
)";

  const char* const html_footer = R"(
      ];

    </script>
  </body>
</html>
)";

 public:
  AnimatedOptimizationVisualizer() {}

  void visualize_into_file(const std::vector<Optimizer::OptimizationStepInfo>& visualized_steps,
                           const std::string& filename) {
    std::ofstream file(filename);
    file << html_header;
    for (const auto& step : visualized_steps) {
      auto visualization = step.visualization;
      boost::replace_all(visualization, "\n", "\\n");
      boost::replace_all(visualization, "\"", "\\\"");
      file << "        {last_applied_rule: \"" << step.last_applied_rule << "\", visualization: \"" << visualization
           << "\"}," << std::endl;
    }
    file << html_footer;
  }
};

}  // namespace opossum