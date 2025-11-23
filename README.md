# gramdata

基本的に、[日本語ウェブコーパス2010のN-gramコーパス](https://www.s-yata.jp/corpus/nwc2010/ngrams/)を参照するためのものだが、まあそれ以外も。

nwc2010/charか、nwc2010/word以下に、入っています。word/over9を基本的に使う想定ではいます。

- [えもじならべあそび](https://ena.hatenablog.jp/entry/20101011/1286787571)
- [新下駄配列](https://y-koutarou.hatenablog.com/entry/2025/05/07/005724)
- [月見草](https://w.atwiki.jp/keylay/pages/16.html)

wikipediaのものは多分以下のどれかですが、正直覚えていない

- [https://gist.github.com/oktopus1959/812559dba7fa1c46c159e8c28788b3f6](https://gist.github.com/oktopus1959/812559dba7fa1c46c159e8c28788b3f6)
- [https://gist.github.com/oktopus1959/ccf2703fbffb84cdad1ff2fdfc38a277](https://gist.github.com/oktopus1959/ccf2703fbffb84cdad1ff2fdfc38a277)
- [https://gist.github.com/oktopus1959/00b2dfe0be5ff1b752c1a174447b9f0f](https://gist.github.com/oktopus1959/00b2dfe0be5ff1b752c1a174447b9f0f)
- [https://gist.github.com/oktopus1959/845cbab989f5d40b9918f1908654fa0f](https://gist.github.com/oktopus1959/845cbab989f5d40b9918f1908654fa0f)
- [https://gist.github.com/oktopus1959/6f5a5e6972dee73fa12381629609926f](https://gist.github.com/oktopus1959/6f5a5e6972dee73fa12381629609926f)

今は[HPLT](https://analytics.hplt-project.org/viewer/HPLT-v3-jpn_Jpan.yaml)とかいうのもあるらしくて、まあノイズの除去がだいぶ面倒くさいだろうけど、できるならかなり強いと思う。~~でもファイルサイズが巨大すぎて、私には扱えません(nwc2010/word/over9時点で扱うのがかなり厳しいのに、3.63TBなんてとても...) ~~スコア10のものに限って落としてみたら、全然圧縮後3GB程度だったので解凍してみました。かなり広告が混じっていてこれでスコア10?と疑問が出はするのですが、まあ参考までに。

~~でもちょっとずつダウンロードして、ノイズは除去して、変換かけて、すでにある結果に足していく、ってやればそれなりに時間は掛かりそうだけどできなくもなさそう 最終的にあり得る文字列というのは7, 80GBに収束していきそうではないですか そんなこと無い?[岡さんがやってくれている](https://x.com/kanchokker/status/1977205036771164291)ので、それを気長に待つかなあ。いやでもこれ2.0だよなあ。~~