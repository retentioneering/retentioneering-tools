/**
 * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
 * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
 * By using, sharing or editing this code you agree with the License terms and conditions.
 * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md
 */

// This file will carry javascript from the Python template (templates.py).
// Solution based on svg-crowbar
function getStyles() {
  let styles = ""
  const styleSheets = document.styleSheets

  function processStyleSheet(ss) {
    if (ss.cssRules) {
      for (let i = 0; i < ss.cssRules.length; i++) {
        const rule = ss.cssRules[i]
        if (rule.type === 3) {
          processStyleSheet(rule.styleSheet)
        } else {
          if (rule.selectorText) {
            if (rule.selectorText.indexOf(">") === -1) {
              styles += "\n" + rule.cssText
            }
          }
        }
      }
    }
  }

  if (styleSheets) {
    for (let i = 0; i < styleSheets.length; i++) {
      processStyleSheet(styleSheets[i])
    }
  }
  return styles
}

function getSource(svg, styles) {
  const doctype = '<?xml version="1.0" standalone="no"?><!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">';
  const prefix = {
    xmlns: "http://www.w3.org/2000/xmlns/",
    xlink: "http://www.w3.org/1999/xlink",
    svg: "http://www.w3.org/2000/svg"
  }

  svg.setAttribute("version", "1.1")

  const defsEl = document.createElement("defs")
  svg.insertBefore(defsEl, svg.firstChild)

  const styleEl = document.createElement("style")
  defsEl.appendChild(styleEl)
  styleEl.setAttribute("type", "text/css")

  svg.removeAttribute("xmlns")
  svg.removeAttribute("xlink")

  if (!svg.hasAttributeNS(prefix.xmlns, "xmlns")) {
    svg.setAttributeNS(prefix.xmlns, "xmlns", prefix.svg)
  }

  if (!svg.hasAttributeNS(prefix.xmlns, "xmlns:xlink")) {
    svg.setAttributeNS(prefix.xmlns, "xmlns:xlink", prefix.xlink)
  }

  const source = (new XMLSerializer()).serializeToString(svg).replace('</style>', '<![CDATA[' + styles + ']]></style>')
  const rect = svg.getBoundingClientRect()

  return {
    top: rect.top,
    left: rect.left,
    width: rect.width,
    height: rect.height,
    class: svg.getAttribute("class"),
    id: svg.getAttribute("id"),
    childElementCount: svg.childElementCount,
    source: [doctype + source]
  }
}

function downloadSVG(sourceSelectorOrEl, filename) {
  const svg = typeof sourceSelectorOrEl === "string" ?
    document.querySelector(sourceSelectorOrEl)
    : sourceSelectorOrEl
  const styles = getStyles() || ""
  const source = getSource(svg, styles)
  const body = document.body

  const url = window.URL.createObjectURL(new Blob(source.source, { "type" : "text\/xml" }))

  const a = document.createElement("a")
  body.appendChild(a)
  a.setAttribute("class", "svg-crowbar")
  a.setAttribute("download", filename + ".svg")
  a.setAttribute("href", url)
  a.style["display"] = "none"
  a.click()

  setTimeout(function() {
    window.URL.revokeObjectURL(url)
  }, 10)
}