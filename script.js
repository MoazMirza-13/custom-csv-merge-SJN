const fs = require("fs");
const csv = require("csv-parser");
const { parse } = require("json2csv");

// Read the parents CSV into a map for quick lookup
const parentsMap = new Map();

fs.createReadStream("db.parents.csv")
  .pipe(csv())
  .on("data", (row) => {
    parentsMap.set(row._id, row.title);
  })
  .on("end", () => {
    const mergedData = [];

    fs.createReadStream("db.chapters.csv")
      .pipe(csv())
      .on("data", (row) => {
        const parentTitle = parentsMap.get(row.parentID) || "";
        mergedData.push({
          id: row._id,
          parentID: row.parentID,
          title: row.title,
          parent_title: parentTitle,
        });
      })
      .on("end", () => {
        // Convert the merged data to CSV
        const csv = parse(mergedData, {
          fields: ["id", "parentID", "title", "parent_title"],
        });

        fs.writeFileSync("merged.csv", csv);

        console.log("CSV files merged successfully.");
      });
  });
