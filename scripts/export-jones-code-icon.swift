import AppKit

enum Segment {
    case move(Double, Double)
    case line(Double, Double)
    case curve(Double, Double, Double, Double, Double, Double)

    var svg: String {
        switch self {
        case let .move(x, y): return "M\(x) \(y)"
        case let .line(x, y): return "L\(x) \(y)"
        case let .curve(a, b, c, d, x, y): return "C\(a) \(b) \(c) \(d) \(x) \(y)"
        }
    }

    func append(to path: CGMutablePath) {
        switch self {
        case let .move(x, y): path.move(to: CGPoint(x: x, y: y))
        case let .line(x, y): path.addLine(to: CGPoint(x: x, y: y))
        case let .curve(a, b, c, d, x, y):
            path.addCurve(to: CGPoint(x: x, y: y), control1: CGPoint(x: a, y: b), control2: CGPoint(x: c, y: d))
        }
    }
}

let letters: [[Segment]] = [
    [.move(246, 616), .curve(246, 710, 424, 710, 424, 604), .line(424, 346),
     .move(318, 346), .line(424, 346)],
    [.move(756, 398), .curve(670, 312, 546, 350, 546, 512),
     .curve(546, 674, 670, 712, 756, 626)],
]
let paths = letters.map { $0.map(\.svg).joined(separator: " ") }
let svg = """
<svg xmlns="http://www.w3.org/2000/svg" width="1024" height="1024" viewBox="0 0 1024 1024" role="img" aria-label="Jones Code JC monogram">
  <defs>
    <linearGradient id="background" x1="0" y1="0" x2="1" y2="1">
      <stop stop-color="#102B4B"/>
      <stop offset="1" stop-color="#25204F"/>
    </linearGradient>
  </defs>
  <rect x="72" y="72" width="880" height="880" rx="200" fill="url(#background)"/>
  <g fill="none" stroke="#FFFFFF" stroke-width="76" stroke-linecap="round" stroke-linejoin="round">
    <path d="\(paths[0])"/>
    <path d="\(paths[1])"/>
  </g>
</svg>

"""

let repo = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
let output = repo.appendingPathComponent("assets/jones-code", isDirectory: true)
try FileManager.default.createDirectory(at: output, withIntermediateDirectories: true)
try svg.write(to: output.appendingPathComponent("jc-mark.svg"), atomically: true, encoding: .utf8)

guard let image = NSBitmapImageRep(bitmapDataPlanes: nil, pixelsWide: 1024, pixelsHigh: 1024,
                                  bitsPerSample: 8, samplesPerPixel: 4, hasAlpha: true,
                                  isPlanar: false, colorSpaceName: .deviceRGB, bytesPerRow: 0, bitsPerPixel: 0),
      let graphics = NSGraphicsContext(bitmapImageRep: image) else {
    fatalError("Could not create the icon bitmap")
}
let context = graphics.cgContext
context.translateBy(x: 0, y: 1024)
context.scaleBy(x: 1, y: -1)
context.saveGState()
context.addPath(CGPath(roundedRect: CGRect(x: 72, y: 72, width: 880, height: 880),
                       cornerWidth: 200, cornerHeight: 200, transform: nil))
context.clip()
let colors = [CGColor(red: 16/255, green: 43/255, blue: 75/255, alpha: 1),
              CGColor(red: 37/255, green: 32/255, blue: 79/255, alpha: 1)]
guard let gradient = CGGradient(colorsSpace: CGColorSpaceCreateDeviceRGB(), colors: colors as CFArray,
                                locations: [0, 1]) else {
    fatalError("Could not create the icon gradient")
}
context.drawLinearGradient(gradient, start: CGPoint(x: 72, y: 72), end: CGPoint(x: 952, y: 952), options: [])
context.restoreGState()
context.setStrokeColor(CGColor(gray: 1, alpha: 1))
context.setLineWidth(76)
context.setLineCap(.round)
context.setLineJoin(.round)
for segments in letters {
    let path = CGMutablePath()
    for segment in segments { segment.append(to: path) }
    context.addPath(path)
    context.strokePath()
}
guard let png = image.representation(using: .png, properties: [:]) else {
    fatalError("Could not encode the icon PNG")
}
try png.write(to: output.appendingPathComponent("jc-macos-1024.png"), options: .atomic)
print("Exported Jones Code SVG and 1024px Mac icon to \(output.path)")
