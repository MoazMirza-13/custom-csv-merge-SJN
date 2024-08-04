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
          isPublished: row.isPublished,
          contentType: row.contentType,
          parent_title: parentData.title || "",
          parent_categories: parentData.categories || "",
          parent_isPublished: parentData.isPublished || "",
          parent_usingScript: parentData.usingScript || "",
        });
      })
      .on("end", () => {
        // Convert the merged data to CSV
        const csvData = parse(mergedData, {
          fields: [
            "id",
            "parentID",
            "title",
            "parent_title",
            "isPublished",
            "parent_isPublished",
            "parent_usingScript",
            "contentType",
            "parent_categories",
          ],
        });

        fs.writeFileSync("merged.csv", csvData);

        console.log("CSV files merged successfully.");

        // Read the categories CSV into a map for quick lookup
        const categoriesMap = new Map();

        fs.createReadStream("db.categories.csv")
          .pipe(csv())
          .on("data", (row) => {
            categoriesMap.set(row._id, row.title);
          })
          .on("end", () => {
            // Read the merged CSV file and add the category titles
            const updatedData = [];

            fs.createReadStream("merged.csv")
              .pipe(csv())
              .on("data", (row) => {
                const categoryTitle =
                  categoriesMap.get(row.parent_categories) || "";
                updatedData.push({
                  ...row,
                  categories_title: categoryTitle,
                });
              })
              .on("end", () => {
                // Convert the updated data to CSV
                const updatedCsv = parse(updatedData, {
                  fields: [
                    "id",
                    "parentID",
                    "title",
                    "parent_title",
                    "isPublished",
                    "parent_isPublished",
                    "parent_usingScript",
                    "contentType",
                    "parent_categories",
                    "categories_title",
                  ],
                });

                fs.writeFileSync("merged_with_categories.csv", updatedCsv);

                console.log("Categories added to merged CSV successfully.");
              });
          });
      });
  });
