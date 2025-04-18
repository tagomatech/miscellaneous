#include <OpenXLSX.hpp>
#include <iostream>
#include <string>
#include <stdexcept>

using namespace OpenXLSX;

int main() {
    const std::string filename = "FILENANE";
    const std::string sheet_name = "SHEET";
    const std::string range_start = "TOP_LEFT_CELL";
    const std::string range_end = "BOTTOM_RIGHT_CELL";

    XLDocument doc;
    try {
        doc.open(filename);
        auto wks = doc.workbook().worksheet(sheet_name);
        auto& cellFormats = doc.styles().cellFormats();
        auto& fills = doc.styles().fills();

        // Define the cell range
        XLCellRange range = wks.range(range_start, range_end);

        std::cout << "Inspecting fill colors from " << range_start << " to " << range_end << "...\n\n";

        for (auto it = range.begin(); it != range.end(); ++it) {
            if (!it.cellExists())
                continue; // skip uninitialized cells

            auto& cell = *it;
            std::string cellAddr = it.address();
            std::string fillHex = "NONE";

            try {
                XLStyleIndex styleIdx = cell.cellFormat();
                if (styleIdx < cellFormats.count()) {
                    XLStyleIndex fillIdx = cellFormats[styleIdx].fillIndex();
                    if (fillIdx < fills.count()) {
                        auto fill = fills[fillIdx];

                        if (fill.fillType() == XLPatternFill) {
                            if (fill.patternType() != XLPatternNone) {
                                fillHex = fill.color().hex();
                            }
                        }
                        else if (fill.fillType() == XLGradientFill) {
                            fillHex = "GRADIENT";
                        }
                    }
                }
            }
            catch (...) {
                fillHex = "ERROR";
            }

            std::cout << "Cell: " << cellAddr << " | Background color: " << fillHex << "\n";
        }

        doc.close();
    }
    catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        if (doc.isOpen()) doc.close();
        return 1;
    }

    return 0;
}
