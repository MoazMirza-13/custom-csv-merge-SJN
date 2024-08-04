const fs = require("fs");
const csv = require("csv-parser");
const { parse } = require("json2csv");

// Read the parents CSV into a map for quick lookup
const parentsMap = new Map();

fs.createReadStream("db.parents.csv")
  .pipe(csv())
  .on("data", (row) => {
    parentsMap.set(row._id, {
      title: row.title,
      categories: row["categories[0]"],
      isPublished: row.isPublished,
      usingScript: row.usingScript,
    });
  })
  .on("end", () => {
    const mergedData = [];

    fs.createReadStream("db.chapters.csv")
      .pipe(csv())
      .on("data", (row) => {
        const parentData = parentsMap.get(row.parentID) || {};
        mergedData.push({
          id: row._id,
          parentID: row.parentID,
          title: row.title,
          parent_title: parentData.title || "",
          parent_categories: parentData.categories || "",
          parent_isPublished: parentData.isPublished || "",
          parent_usingScript: parentData.usingScript || "",
        });
      })
      .on("end", () => {
        // Convert the merged data to CSV
        const csv = parse(mergedData, {
          fields: [
            "id",
            "title",
            "parentID",
            "parent_title",
            "parent_categories",
            "parent_isPublished",
            "parent_usingScript",
          ],
        });

        fs.writeFileSync("merged.csv", csv);

        console.log("CSV files merged successfully.");
      });
  });
