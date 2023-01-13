const sidebars = {
  sidebar: [
    {
      type: "category",
      label: "ZIO Connect",
      collapsed: false,
      link: { type: "doc", id: "index" },
      items: [
		'couchbase-connector',
		'dynamodb-connector',
		'file-connector',
		's3-connector',
      ]
    }
  ]
};

module.exports = sidebars;
