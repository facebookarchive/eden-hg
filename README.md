# eden-hg

This is the Mercurial extension for
[Eden](https://github.com/facebookexperimental/eden).

This extension should be bundled with Eden by default so that after running
`eden clone <hg-repository> <directory>`, the resulting
`<hg-repository>/.hg/hgrc` file will have the following line:

```
[extensions]
eden = /path/to/eden-hg-bundled-with-eden/eden/hg/eden
```

If you want to use your own version of this extension, change this config to:

```
[extensions]
eden = /path/to/your-clone-of-eden-hg/eden/hg/eden
```
