set nohls
set tags+=$HOME/bak/tags
set fileencoding=utf-8
set fileencodings=utf-8,ucs-bom,gb18030,gbk,gb2312,cp936
set termencoding=utf-8
set encoding=utf-8
set ts=4
set nocompatible
set paste
set tabstop=4
set shiftwidth=4
set showmatch
set autoindent
set cindent
set smartindent
set vb t_vb=
set formatoptions+=mM
syntax on

color default

"设置自动补全
let g:neocomplcache_enable_at_startup = 1 

"进行版权声明的设置, 添加或更新头
map <F4> :call TitleDet()<cr>
function AddTitle() 
    call append(0,"/**")
    call append(1," * Program   : ".expand("%:t"))
    call append(2," * Author    : Alex.Wang")
    call append(3," * Write Date: ".strftime("%Y-%m-%d %H:%M"))
    call append(4," * Modi      : ")
    call append(5," * ModiDate  : ")
    call append(6," * Comment   : ")
    call append(7," *")
    call append(8," */")
    echohl WarningMsg | echo "Successful in adding the copyright." | echohl None
endf
"更新最近修改时间和文件名
function UpdateTitle()
    execute ' /* ModiDate/s@:.*$@\=strftime(": %Y-%m-%d %H:%M")@'
    execute ' /* Modi /s@:.*$@\=": ".expand("Alex.Wang")@'
    execute ' /* Program/s@:.*$@\=": ".expand("%:t")@'
    echohl WarningMsg | echo "Successful in updating the copyright." | echohl None
endfunction
"判断前10行代码里面，是否有ModiDate这个单词，
"如果没有的话，代表没有添加过作者信息，需要新添加；
"如果有的话，那么只需要更新即可
function TitleDet()
    let n=1
    "默认为添加
    while n < 10
        let line = getline(n)
        if line =~ '[\s\S]*ModiDate[\s\S]*'
            call UpdateTitle()
            return
        endif
        let n = n + 1
    endwhile
    call AddTitle()
endfunction
