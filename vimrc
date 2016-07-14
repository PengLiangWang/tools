set nu
set expandtab
set tabstop=4
set shiftwidth=4
set showmatch
set autoindent
set cindent
set smartindent
set vb t_vb=
set formatoptions+=mM
syntax on  "�﷨��ɫ
set fileencoding=utf-8
set fileencodings=utf-8,ucs-bom,gb18030,gbk,gb2312,cp936
set termencoding=utf-8
set encoding=utf-8

color default

"�����Զ���ȫ
let g:neocomplcache_enable_at_startup = 1 

"���а�Ȩ����������, ��ӻ����ͷ
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
"��������޸�ʱ����ļ���
function UpdateTitle()
    execute ' /* ModiDate/s@:.*$@\=strftime(": %Y-%m-%d %H:%M")@'
    execute ' /* Modi /s@:.*$@\=": ".expand("Alex.Wang")@'
    execute ' /* Program/s@:.*$@\=": ".expand("%:t")@'
    echohl WarningMsg | echo "Successful in updating the copyright." | echohl None
endfunction
"�ж�ǰ10�д������棬�Ƿ���ModiDate������ʣ�
"���û�еĻ�������û����ӹ�������Ϣ����Ҫ����ӣ�
"����еĻ�����ôֻ��Ҫ���¼���
function TitleDet()
    let n=1
    "Ĭ��Ϊ���
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
