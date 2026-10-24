import Foundation
import XCTest

/// A single finite journey. Backend assertions live in the owning Node fixture;
/// a UI step advances only after its authenticated server readback succeeds.
final class PinWorkflowTests: XCTestCase {
    private var bridgeURL = ""
    private var bridgeToken = ""

    override func setUpWithError() throws {
        continueAfterFailure = false
        let environment = ProcessInfo.processInfo.environment
        bridgeURL = try XCTUnwrap(environment["JONES_E2E_URL"])
        bridgeToken = try XCTUnwrap(environment["JONES_E2E_TOKEN"])
        XCTAssertTrue(bridgeURL.hasPrefix("http://127.0.0.1:"))
    }

    private func exchange(_ path: String, phase: String? = nil, image: Data? = nil) throws -> [String: Any] {
        var request = URLRequest(url: try XCTUnwrap(URL(string: bridgeURL + path)))
        request.timeoutInterval = 25
        request.setValue("Bearer " + bridgeToken, forHTTPHeaderField: "Authorization")
        if let phase {
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try JSONSerialization.data(withJSONObject: ["phase": phase])
        }
        if let image {
            request.httpMethod = "POST"
            request.setValue("image/png", forHTTPHeaderField: "Content-Type")
            request.httpBody = image
        }
        let done = expectation(description: "Owned fixture response")
        var payload: Data?
        var status: Int?
        var failed = false
        URLSession.shared.dataTask(with: request) { data, response, error in
            payload = data
            status = (response as? HTTPURLResponse)?.statusCode
            failed = error != nil
            done.fulfill()
        }.resume()
        wait(for: [done], timeout: 30)
        XCTAssertFalse(failed, "Fixture transport failed")
        XCTAssertEqual(status, 200, "Backend phase oracle rejected the native action")
        return try XCTUnwrap(JSONSerialization.jsonObject(with: XCTUnwrap(payload)) as? [String: Any])
    }

    @MainActor
    private func unique(_ query: XCUIElementQuery) throws -> XCUIElement {
        XCTAssertTrue(query.firstMatch.waitForExistence(timeout: 15), "Expected native element missing")
        XCTAssertEqual(query.count, 1, "Ambiguous native selector")
        return query.element(boundBy: 0)
    }

    @MainActor
    private func openMenu(_ app: XCUIApplication, title: String, expecting label: String) throws -> XCUIElement {
        let row = try unique(app.staticTexts.matching(NSPredicate(format: "label == %@", title)))
        row.press(forDuration: 1.1)
        return try unique(app.buttons.matching(NSPredicate(format: "label == %@", label)))
    }

    @MainActor
    func testPinPersistence() throws {
        let config = try exchange("/configuration")
        let app = XCUIApplication(bundleIdentifier: try XCTUnwrap(config["bundleId"] as? String))
        let title = try XCTUnwrap(config["title"] as? String)
        app.launch()
        defer { app.terminate() }
        try unique(app.buttons.matching(NSPredicate(format: "label == %@", "Add environment"))).tap()
        try unique(app.textFields.matching(NSPredicate(format: "label == %@", "Address"))).tap()
        app.textFields["Address"].typeText(try XCTUnwrap(config["origin"] as? String))
        try unique(app.textFields.matching(NSPredicate(format: "label == %@", "Code"))).tap()
        // XCTest may log typed text. The wrapper keeps raw results only in the
        // private disposable run root; it exports no pairing-screen captures.
        app.textFields["Code"].typeText(try XCTUnwrap(config["pairingToken"] as? String))
        try unique(app.buttons.matching(NSPredicate(format: "label == %@", "Add environment"))).tap()
        _ = try unique(app.staticTexts.matching(NSPredicate(format: "label == %@", title)))
        _ = try exchange("/phase", phase: "initial")
        if config["noAction"] as? Bool != true {
            try openMenu(app, title: title, expecting: "Pin").tap()
        }
        // Deliberately unchanged in the no-action variant: this must fail.
        _ = try exchange("/phase", phase: "pinned")
        app.terminate()
        app.launch()
        let unpin = try openMenu(app, title: title, expecting: "Unpin")
        _ = try exchange("/phase", phase: "pinnedRelaunch")
        _ = try exchange("/capture/pinnedRelaunch", image: app.screenshot().pngRepresentation)
        unpin.tap()
        _ = try exchange("/phase", phase: "unpinned")
        app.terminate()
        app.launch()
        _ = try openMenu(app, title: title, expecting: "Pin")
        _ = try exchange("/phase", phase: "unpinnedRelaunch")
        _ = try exchange("/capture/unpinnedRelaunch", image: app.screenshot().pngRepresentation)
        _ = try exchange("/phase", phase: "complete")
    }
}
