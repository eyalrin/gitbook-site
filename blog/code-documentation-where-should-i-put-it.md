---
description: You know the long-running debate around where code documentation should live?
cover: ../.gitbook/assets/diagrams-post-header.png
coverY: 0
---

# Code Documentation – Where Should I Put It?

One of the strongest arguments against keeping documentation alongside the code itself (i.e., inside Git) has always been system design diagrams. Yes, tools like Mermaid exist, but let’s be honest – the generated diagrams often look overly simplistic at best and cluttered at worst. Most of us are used to tools like Miro, draw.io, or Lucidchart, but that also means the diagrams live separately from the code they describe, which is far from ideal.

Yesterday I discovered a really elegant solution to this problem.

If you’ve used [Excalidraw](https://github.com/excalidraw/excalidraw) – the popular open-source diagramming tool – it turns out it has a very useful capability: you can export diagrams as `PNG` files that also embed the diagram’s source data inside the image itself.

That means you can:

* Store the diagram directly inside your repository
* View it as a regular image file on GitHub (This is where other tools fail)
* Edit it visually with full drag-and-drop support
* And still manage changes through Git like any other project asset

All you need is the Excalidraw extension for VS Code or JetBrains IDEs, and you can edit diagrams directly from your development environment.

In my opinion, this solves the “where should documentation live?” debate in a really clean way — especially for architecture and system design diagrams.
